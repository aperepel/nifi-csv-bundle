package org.apache.nifi.processors.lookup;

import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.annotation.behavior.EventDriven;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.expression.AttributeExpression;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.lookup.LookupTableService;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;

import java.io.ByteArrayOutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

@EventDriven
@SupportsBatching
@Tags({"map", "cache", "lookup"})
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@CapabilityDescription("Puts a value in a lookup table")
@SeeAlso(LookupTableService.class)
public class PutLookupTable extends AbstractProcessor {

    public static final PropertyDescriptor PROP_LOOKUP_TABLE_SERVICE = new PropertyDescriptor.Builder()
                                                                               .name("Lookup table service")
                                                                               .identifiesControllerService(LookupTableService.class)
                                                                               .required(true)
                                                                               .build();

    public static final PropertyDescriptor PROP_LOOKUP_ENTRY_ID = new PropertyDescriptor.Builder()
                                                                          .name("Lookup Entry Identifier")
                                                                          .description("A FlowFile attribute, or the results of an Attribute Expression Language statement, which will " +
                                                                                               "be evaluated against a FlowFile in order to determine the key")
                                                                          .required(true)
                                                                          .addValidator(StandardValidators.createAttributeExpressionLanguageValidator(AttributeExpression.ResultType.STRING, true))
                                                                          .defaultValue("${hash.value}")
                                                                          .expressionLanguageSupported(true)
                                                                          .build();

    public static final AllowableValue STRATEGY_REPLACE = new AllowableValue("replace", "Replace if present",
                                                                                        "Adds the specified entry to the lookup table, replacing any value that is currently set.");

    public static final AllowableValue STRATEGY_KEEP_ORIGINAL = new AllowableValue("keeporiginal", "Keep original",
                                                                                              "Adds the specified entry to the lookup table, if the key does not exist.");

    public static final PropertyDescriptor PROP_LOOKUP_TABLE_UPDATE_STRATEGY = new PropertyDescriptor.Builder()
                                                                                       .name("Lookup table update strategy")
                                                                                       .description("Determines how the lookup table is updated if it already contains the entry")
                                                                                       .required(true)
                                                                                       .allowableValues(STRATEGY_REPLACE, STRATEGY_KEEP_ORIGINAL)
                                                                                       .defaultValue(STRATEGY_REPLACE.getValue())
                                                                                       .build();

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
                                                           .name("success")
                                                           .description("Any FlowFile that is successfully inserted into the lookup table will be routed to this relationship")
                                                           .build();

    public static final Relationship REL_FAILURE = new Relationship.Builder()
                                                           .name("failure")
                                                           .description("Any FlowFile that cannot be inserted into the lookup table will be routed to this relationship")
                                                           .build();


    private List<PropertyDescriptor> descriptors;

    private Set<Relationship> relationships;

    @Override
    protected void init(final ProcessorInitializationContext context) {
        final List<PropertyDescriptor> descriptors = new ArrayList<PropertyDescriptor>();
        descriptors.add(PROP_LOOKUP_TABLE_SERVICE);
        descriptors.add(PROP_LOOKUP_ENTRY_ID);
        descriptors.add(PROP_LOOKUP_TABLE_UPDATE_STRATEGY);
        this.descriptors = Collections.unmodifiableList(descriptors);

        final Set<Relationship> relationships = new HashSet<Relationship>();
        relationships.add(REL_SUCCESS);
        relationships.add(REL_FAILURE);
        this.relationships = Collections.unmodifiableSet(relationships);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return this.relationships;
    }

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return descriptors;
    }

    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        final String lookupKey = context.getProperty(PROP_LOOKUP_ENTRY_ID).evaluateAttributeExpressions(flowFile).getValue();

        // if the computed value is null, or empty, we transfer the flow file to failure relationship
        if (StringUtils.isBlank(lookupKey)) {
            getLogger().error("FlowFile {} has no attribute for given Cache Entry Identifier", new Object[]{flowFile});
            flowFile = session.penalize(flowFile);
            session.transfer(flowFile, REL_FAILURE);
            return;
        }

        final LookupTableService lookup = context.getProperty(PROP_LOOKUP_TABLE_SERVICE).asControllerService(LookupTableService.class);
        final String updateStrategy = context.getProperty(PROP_LOOKUP_TABLE_UPDATE_STRATEGY).getValue();

        boolean cached = false;

        // get flow file content
        final ByteArrayOutputStream baos = new ByteArrayOutputStream();
        session.exportTo(flowFile, baos);

        if (updateStrategy.equals(STRATEGY_REPLACE.getValue())) {
            lookup.put(lookupKey, baos.toString());
            cached = true;
        } else if (updateStrategy.equals(STRATEGY_KEEP_ORIGINAL.getValue())) {
            String oldValue = lookup.putIfAbsent(lookupKey, baos.toString());

            if (oldValue == null) {
                cached = true;
            }
        }

        if (cached) {
            session.transfer(flowFile, REL_SUCCESS);
            if (getLogger().isTraceEnabled()) {
                getLogger().trace("Updated {}={}", new Object[] {lookupKey, baos.toString()});
            }
        } else {
            session.transfer(flowFile, REL_FAILURE);
            if (getLogger().isTraceEnabled()) {
                getLogger().trace("Not updating " + lookupKey);
            }

        }
    }

}
