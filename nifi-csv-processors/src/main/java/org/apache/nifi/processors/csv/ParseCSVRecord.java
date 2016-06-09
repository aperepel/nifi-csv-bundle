/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.nifi.processors.csv;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.LineIterator;
import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.ReadsAttribute;
import org.apache.nifi.annotation.behavior.ReadsAttributes;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.InputStreamCallback;
import org.apache.nifi.processor.util.StandardValidators;

import java.io.IOException;
import java.io.InputStream;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import static java.nio.charset.StandardCharsets.UTF_8;

@Tags({"csv", "tab", "excel"})
@SupportsBatching
@CapabilityDescription("Extract a header from a delimited file and save it in an attribute. Also maintains a list and count of column headers.")
@SeeAlso({ExtractCSVHeader.class})
@ReadsAttributes(
        {
                @ReadsAttribute(attribute = "delimited.header.original", description = "Header line as is"),
                @ReadsAttribute(attribute = "delimited.header.column.N", description = "A common attribute prefix. Every recognized column will be " +
                                                                                               "listed with a 1-based index"),
                @ReadsAttribute(attribute = "delimited.header.columnCount", description = "Header line as is")
        })
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
public class ParseCSVRecord extends AbstractCSVProcessor {

    public static final String DEFAULT_VALUE_ATTR_PREFIX = "delimited.column.";

    public static final PropertyDescriptor PROP_RECORD_FROM_ATTRIBUTE = new PropertyDescriptor.Builder()
                                                                            .name("Record Source From Attribute")
                                                                            .description("When set, the delimited record will be read from this flowfile attribute (don't include EL syntax curly braces or $)")
                                                                            .required(false)
                                                                            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
                                                                            .build();

    public static final PropertyDescriptor PROP_VALUE_ATTR_PREFIX = new PropertyDescriptor.Builder()
                                                                            .name("Output Attribute Prefix")
                                                                            .description("Delimited record columns will be added using this prefix for better grouping.")
                                                                            .required(true)
                                                                            .expressionLanguageSupported(true)
                                                                            .defaultValue(DEFAULT_VALUE_ATTR_PREFIX)
                                                                            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
                                                                            .build();


    public static final PropertyDescriptor PROP_SCHEMA_ATTR_PREFIX = new PropertyDescriptor.Builder()
                                                                             .name("Input Schema Attribute Prefix")
                                                                             .description("Use a delimited schema previously extracted by ExtractCSVHeader")
                                                                             .required(true)
                                                                             .expressionLanguageSupported(true)
                                                                             .defaultValue(DEFAULT_SCHEMA_ATTR_PREFIX)
                                                                             .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
                                                                             .build();

    public static final PropertyDescriptor PROP_TRIM_VALUES = new PropertyDescriptor.Builder()
                                                                             .name("Trim Values")
                                                                             .description("Remove leading and trailing spaces from column value records (note, header is not trimmed)")
                                                                             .required(true)
                                                                             .allowableValues(Boolean.TRUE.toString(), Boolean.FALSE.toString())
                                                                             .defaultValue("false")
                                                                             .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
                                                                             .build();


    public static final Relationship REL_SUCCESS = new Relationship.Builder()
                                                           .name("success")
                                                           .description("Successful parse result")
                                                           .build();


    public static final Relationship REL_FAILURE = new Relationship.Builder()
                                                           .name("failure")
                                                           .description("Error in incoming data")
                                                           .build();

    private List<PropertyDescriptor> descriptors;

    private Set<Relationship> relationships;

    @Override
    protected void init(final ProcessorInitializationContext context) {
        final List<PropertyDescriptor> descriptors = new ArrayList<PropertyDescriptor>();
        descriptors.add(PROP_FORMAT);
        descriptors.add(PROP_DELIMITER);
        descriptors.add(PROP_RECORD_FROM_ATTRIBUTE);
        descriptors.add(PROP_SCHEMA_ATTR_PREFIX);
        descriptors.add(PROP_VALUE_ATTR_PREFIX);
        descriptors.add(PROP_TRIM_VALUES);
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
    protected Collection<ValidationResult> customValidate(ValidationContext validationContext) {
        final List<ValidationResult> validationResults = new ArrayList<>(super.customValidate(validationContext));

        String delimiter = validationContext.getProperty(PROP_DELIMITER).getValue();
        // guaranteed to be non-null by now
        if (delimiter != null) {
            if (delimiter.length() > 1) {
                ValidationResult result = StandardValidators.ATTRIBUTE_EXPRESSION_LANGUAGE_VALIDATOR
                                                  .validate(PROP_DELIMITER.getDisplayName(), delimiter, validationContext);
                if (!result.isValid()) {
                    validationResults.add(new ValidationResult.Builder()
                                                  .subject(PROP_DELIMITER.getDisplayName())
                                                  .explanation("Only NiFI Expression or or single-byte delimiters are supported")
                                                  .build()
                    );
                }
            }
        }

        return validationResults;
    }

    @OnScheduled
    public void onScheduled(final ProcessContext context) {

    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        final FlowFile original = session.get();
        if (original == null) {
            return;
        }

        final AtomicBoolean lineFound = new AtomicBoolean(false);
        final Map<String, String> outputAttrs = new HashMap<>();

        session.read(original, new InputStreamCallback() {
            @Override
            public void process(InputStream inputStream) throws IOException {
                final String fromAttribute = context.getProperty(PROP_RECORD_FROM_ATTRIBUTE).getValue();

                String unparsedRecord;
                // data source is the attribute
                if (StringUtils.isNotBlank(fromAttribute)) {
                    unparsedRecord = original.getAttribute(fromAttribute);
                    if (StringUtils.isBlank(unparsedRecord)) {
                        // will be routed to failure at the end of the method implementation
                        return;
                    }
                } else {
                    // data source is the content
                    // TODO expose the charset property?
                    LineIterator iterator = IOUtils.lineIterator(inputStream, UTF_8);
                    if (!iterator.hasNext()) {
                        return;
                    }
                    unparsedRecord = iterator.next();
                }

                lineFound.set(true);
                final String format = context.getProperty(PROP_FORMAT).getValue();
                final String delimiter = context.getProperty(PROP_DELIMITER).evaluateAttributeExpressions(original).getValue();
                final String schemaPrefix = context.getProperty(PROP_SCHEMA_ATTR_PREFIX).evaluateAttributeExpressions(original).getValue();
                final String valuePrefix = context.getProperty(PROP_VALUE_ATTR_PREFIX).evaluateAttributeExpressions(original).getValue();
                final boolean trimValues = context.getProperty(PROP_TRIM_VALUES).asBoolean();

                final CSVFormat csvFormat = buildFormat(format,
                        delimiter,
                        false, // this is a payload, not header anymore
                        null); // no custom header

                final CSVParser parser = csvFormat.parse(new StringReader(unparsedRecord));
                List<CSVRecord> records = parser.getRecords();
                if (records.size() > 1) {
                    // TODO revisit for NiFi's native micro-batching
                    throw new ProcessException("Multi-line entries not supported");
                }

                CSVRecord record = records.get(0);

                Map<String, String> originalAttrs = original.getAttributes();
                // filter delimited schema attributes only
                Map<String, String> schemaAttrs = new HashMap<>();
                for (String key : originalAttrs.keySet()) {
                    if (key.startsWith(schemaPrefix)) {
                        schemaAttrs.put(key, originalAttrs.get(key));
                    }
                }

                // put key/value pairs into attributes
                for (int i = 0; i < record.size(); i++) {
                    String columnName = schemaAttrs.get(schemaPrefix + (i + 1)); // 1-based column numbering
                    if (columnName == null) {
                        // 1-based column index
                        columnName = String.valueOf(i + 1);
                    }
                    // TODO indexed schemaless parsing vs auto-schema vs user-provided schema
                    String columnValue = record.get(i);
                    if (trimValues) {
                        columnValue = columnValue.trim();
                    }
                    String attrName = (StringUtils.isBlank(valuePrefix)
                                           ? "delimited.column."
                                           : valuePrefix) + columnName;
                    outputAttrs.put(attrName, columnValue);
                }
            }
        });

        if (lineFound.get()) {
            FlowFile ff = session.putAllAttributes(original, outputAttrs);
            session.transfer(ff, REL_SUCCESS);
        } else {
            session.transfer(original, REL_FAILURE);
        }
    }

}
