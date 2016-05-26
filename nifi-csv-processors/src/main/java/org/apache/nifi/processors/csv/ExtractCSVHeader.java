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
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.LineIterator;
import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
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

@Tags({"csv"})
@CapabilityDescription("Extract a header from a delimited file and save it in an attribute. Also maintains a list and count of column headers.")
@SeeAlso({ParseCSV.class})
@WritesAttributes(
        {
                @WritesAttribute(attribute = "delimited.header.original", description = "Header line as is"),
                @WritesAttribute(attribute = "delimited.header.column.N", description = "A common attribute prefix. Every recognized column will be " +
                                                                                                "listed with a 1-based index"),
                @WritesAttribute(attribute = "delimited.header.columnCount", description = "Header line as is")
        })
public class ExtractCSVHeader extends AbstractProcessor {

    public static final String ATTR_HEADER_ORIGINAL = "delimited.header.original";

    public static final String ATTR_HEADER_COLUMN_COUNT = "delimited.header.columnCount";

    public static final String ATTR_HEADER_COLUMN_PREFIX = "delimited.header.column.";

    public static final AllowableValue VALUE_CSV = new AllowableValue("CSV", "CSV", "Standard comma-separated format.");

    public static final AllowableValue VALUE_EXCEL = new AllowableValue("EXCEL", "EXCEL", "Excel file format (using a comma as the value delimiter). Note that the actual " +
                                                                                                  "value delimiter used by Excel is locale dependent, it might be necessary to customize " +
                                                                                                  "this format to accommodate to your regional settings.");
    public static final AllowableValue VALUE_RFC4180 = new AllowableValue("RFC4180", "RFC4180", "Common Format and MIME Type for Comma-Separated Values (CSV) Files");
    public static final AllowableValue VALUE_TAB = new AllowableValue("TAB", "TAB", "Tab-delimited format.");
    public static final AllowableValue VALUE_MYSQL = new AllowableValue("MYSQL", "MYSQL", "Default MySQL format used " +
                                                                                                  "by the {@code SELECT INTO OUTFILE} and {@code LOAD DATA INFILE} operations.");

    public static final PropertyDescriptor PROP_FORMAT = new PropertyDescriptor
                                                                     .Builder().name("Delimited Format")
                                                                 .description("Example Property")
                                                                 .required(true)
                                                                 .defaultValue(VALUE_EXCEL.getValue())
                                                                 .allowableValues(VALUE_EXCEL, VALUE_CSV, VALUE_RFC4180, VALUE_TAB, VALUE_MYSQL)
                                                                 .expressionLanguageSupported(true)
                                                                 .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
                                                                 .build();

    public static final PropertyDescriptor PROP_DELIMITER = new PropertyDescriptor
                                                                        .Builder().name("Column Delimiter")
                                                                    .description("Column Delimiter")
                                                                    .required(false)
                                                                    .expressionLanguageSupported(true)
                                                                    .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
                                                                    .build();


    public static final Relationship REL_SUCCESS = new Relationship.Builder()
                                                           .name("success")
                                                           .description("Successfully extracted the header")
                                                           .build();

    public static final Relationship REL_FAILURE = new Relationship.Builder()
                                                           .name("failure")
                                                           .description("Error with incoming data")
                                                           .build();

    private List<PropertyDescriptor> descriptors;

    private Set<Relationship> relationships;

    private static final Map<String, CSVFormat> supportedFormats = new HashMap<>();

    static {
        // we are using more user-friendly names in the UI
        supportedFormats.put(VALUE_EXCEL.getValue(), CSVFormat.EXCEL); // our default
        supportedFormats.put(VALUE_CSV.getValue(), CSVFormat.DEFAULT);
        supportedFormats.put(VALUE_TAB.getValue(), CSVFormat.TDF);
        supportedFormats.put(VALUE_RFC4180.getValue(), CSVFormat.RFC4180);
        supportedFormats.put(VALUE_MYSQL.getValue(), CSVFormat.MYSQL);
    }

    @Override
    protected void init(final ProcessorInitializationContext context) {
        final List<PropertyDescriptor> descriptors = new ArrayList<PropertyDescriptor>();
        descriptors.add(PROP_FORMAT);
        descriptors.add(PROP_DELIMITER);
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
        final FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        final AtomicBoolean lineFound = new AtomicBoolean(false);
        final Map<String, String> attrs = new HashMap<>();


        session.read(flowFile, new InputStreamCallback() {
            @Override
            public void process(InputStream inputStream) throws IOException {
                // TODO expose the charset property?
                LineIterator iterator = IOUtils.lineIterator(inputStream, UTF_8);
                if (iterator.hasNext()) {
                    lineFound.set(true);
                    final String header = iterator.nextLine();
                    attrs.put(ATTR_HEADER_ORIGINAL, header);


                    final String format = context.getProperty(PROP_FORMAT).evaluateAttributeExpressions(flowFile).getValue();
                    final String delimiter = context.getProperty(PROP_DELIMITER).evaluateAttributeExpressions(flowFile).getValue();
                    // TODO validate delimiter in the callback first
                    final CSVFormat csvFormat = buildFormat(format,
                            delimiter,
                            true,  // we assume first line is the header
                            null); // no custom header
                    final CSVParser parser = csvFormat.parse(new StringReader(header));
                    final Map<String, Integer> headers = parser.getHeaderMap();
                    final int columnCount = headers.size();
                    attrs.put(ATTR_HEADER_COLUMN_COUNT, String.valueOf(columnCount));
                    for (Map.Entry<String, Integer> h : headers.entrySet()) {
                        // CSV columns are 1-based in Excel
                        attrs.put(ATTR_HEADER_COLUMN_PREFIX + (h.getValue() + 1), h.getKey());
                    }
                }
            }
        });

        if (lineFound.get()) {
            FlowFile ff = session.putAllAttributes(flowFile, attrs);
            session.transfer(ff, REL_SUCCESS);
        } else {
            session.transfer(flowFile, REL_FAILURE);
        }
    }

    protected CSVFormat buildFormat(String format, String delimiter, boolean withHeader, String customHeader) {
        CSVFormat csvFormat = supportedFormats.get(format);
        if (csvFormat == null) {
            throw new IllegalArgumentException("Unsupported delimited format: " + format);
        }

        if (withHeader & customHeader != null) {
            csvFormat = csvFormat.withSkipHeaderRecord(true);
            csvFormat = csvFormat.withHeader(customHeader);
        } else if (withHeader & customHeader == null) {
            csvFormat = csvFormat.withHeader();
        }

        // don't use isNotBlank() here, as it strips tabs too
        if (StringUtils.isNotEmpty(delimiter)) {
            csvFormat = csvFormat.withDelimiter(delimiter.charAt(0));
        }
        return csvFormat;
    }
}
