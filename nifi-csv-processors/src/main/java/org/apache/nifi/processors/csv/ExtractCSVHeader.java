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
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.InputStreamCallback;

import java.io.IOException;
import java.io.InputStream;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import static java.nio.charset.StandardCharsets.UTF_8;

@Tags({"csv"})
@CapabilityDescription("Extract a header from a delimited file and save it in an attribute.")
@SeeAlso({ParseCSV.class})
@WritesAttributes(
        {
                @WritesAttribute(attribute = "delimited.header.original", description = "Header line as is"),
//                @WritesAttribute(attribute = "delimited.header.columns", description = "Column count after the header is parsed"),
                @WritesAttribute(attribute = "delimited.header.column.count", description = "Header line as is")
        })
public class ExtractCSVHeader extends AbstractProcessor {

    public static final String ATTR_HEADER_ORIGINAL = "delimited.header.original";

    public static final String ATTR_HEADER_COLUMN_COUNT = "delimited.header.columnCount";

    public static final String ATTR_HEADER_COLUMN_PREFIX = "delimited.header.column.";


//    public static final PropertyDescriptor MY_PROPERTY = new PropertyDescriptor
//                                                                     .Builder().name("My Property")
//                                                                 .description("Example Property")
//                                                                 .required(false)
//                                                                 .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
//                                                                 .build();

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

    @Override
    protected void init(final ProcessorInitializationContext context) {
        final List<PropertyDescriptor> descriptors = new ArrayList<PropertyDescriptor>();
//        descriptors.add(MY_PROPERTY);
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

    @OnScheduled
    public void onScheduled(final ProcessContext context) {

    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();
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

                    // TODO expose format as a property
                    final CSVParser parser = CSVFormat.EXCEL.withFirstRecordAsHeader().parse(new StringReader(header));
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
            flowFile = session.putAllAttributes(flowFile, attrs);
            session.transfer(flowFile, REL_SUCCESS);
        } else {
            session.transfer(flowFile, REL_FAILURE);
        }
    }
}
