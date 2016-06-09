package org.apache.nifi.processors.lookup;

import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.annotation.behavior.EventDriven;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
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
import org.apache.nifi.processor.io.OutputStreamCallback;
import org.apache.nifi.processor.util.StandardValidators;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

@EventDriven
@SupportsBatching
@Tags({"map", "cache", "fetch", "lookup"})
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@CapabilityDescription("Looks up a value using the lookup service")
@SeeAlso(LookupTableService.class)
public class GetLookupTable extends AbstractProcessor {


    public static final PropertyDescriptor PROP_LOOKUP_TABLE_SERVICE = new PropertyDescriptor.Builder()
                                                                               .name("Lookup table service")
                                                                               .identifiesControllerService(LookupTableService.class)
                                                                               .required(true)
                                                                               .build();

    public static final PropertyDescriptor PROP_LOOKUP_ENTRY_ID = new PropertyDescriptor.Builder()
                                                                          .name("Lookup Entry Identifier")
                                                                          .description("A FlowFile attribute, or the results of an Attribute Expression Language statement, which will be evaluated "
                                                                                               + "against a FlowFile in order to determine the lookup key")
                                                                          .required(true)
                                                                          .addValidator(StandardValidators.createAttributeExpressionLanguageValidator(AttributeExpression.ResultType.STRING, true))
                                                                          .defaultValue("${hash.value}")
                                                                          .expressionLanguageSupported(true)
                                                                          .build();

    public static final PropertyDescriptor PROP_PUT_CACHE_VALUE_IN_ATTRIBUTE = new PropertyDescriptor.Builder()
                                                                                       .name("Put Result Value In Attribute")
                                                                                       .description("If set, the lookup result value will be put into an attribute of the FlowFile instead of the content of the"
                                                                                                            + "FlowFile. The attribute key to put to is determined by evaluating value of this property.")
                                                                                       .addValidator(StandardValidators.createAttributeExpressionLanguageValidator(AttributeExpression.ResultType.STRING))
                                                                                       .expressionLanguageSupported(true)
                                                                                       .build();

    public static final Relationship REL_FOUND = new Relationship.Builder()
                                                         .name("found")
                                                         .build();


    public static final Relationship REL_NOT_FOUND = new Relationship.Builder()
                                                             .name("not found")
                                                             .build();

    public static final Relationship REL_FAILURE = new Relationship.Builder()
                                                           .name("failure")
                                                           .description("If there's a data or configuration problem")
                                                           .build();


    private List<PropertyDescriptor> descriptors;

    private Set<Relationship> relationships;

    @Override
    protected void init(final ProcessorInitializationContext context) {
        final List<PropertyDescriptor> descriptors = new ArrayList<PropertyDescriptor>();
        descriptors.add(PROP_LOOKUP_TABLE_SERVICE);
        descriptors.add(PROP_LOOKUP_ENTRY_ID);
        descriptors.add(PROP_PUT_CACHE_VALUE_IN_ATTRIBUTE);
        this.descriptors = Collections.unmodifiableList(descriptors);

        final Set<Relationship> relationships = new HashSet<Relationship>();
        relationships.add(REL_FOUND);
        relationships.add(REL_NOT_FOUND);
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
        if (StringUtils.isBlank(lookupKey)) {
            getLogger().error("FlowFile {} has no attribute for a given lookup key", new Object[]{flowFile});
            flowFile = session.penalize(flowFile);
            session.transfer(flowFile, REL_FAILURE);
            return;
        }
        LookupTableService lookup = context.getProperty(PROP_LOOKUP_TABLE_SERVICE).asControllerService(LookupTableService.class);
        final String result = lookup.get(lookupKey);
        if (getLogger().isTraceEnabled()) {
            getLogger().trace("Lookup result for key '{}': {}", new Object[]{lookupKey, result});
        }
        if (result == null) {
            session.transfer(flowFile, REL_NOT_FOUND);
            getLogger().debug("Could not find an entry in lookup table for {}; routing to not-found", new Object[]{flowFile});
            return;
        } else {
            boolean putInAttribute = context.getProperty(PROP_PUT_CACHE_VALUE_IN_ATTRIBUTE).isSet();
            if (putInAttribute) {
                String attributeName = context.getProperty(PROP_PUT_CACHE_VALUE_IN_ATTRIBUTE).evaluateAttributeExpressions(flowFile).getValue();
                flowFile = session.putAttribute(flowFile, attributeName, result);
            } else {
                flowFile = session.write(flowFile, new OutputStreamCallback() {
                    @Override
                    public void process(OutputStream out) throws IOException {
                        out.write(result.getBytes(StandardCharsets.UTF_8));
                    }
                });
            }
        }

        session.transfer(flowFile, REL_FOUND);
    }
}
