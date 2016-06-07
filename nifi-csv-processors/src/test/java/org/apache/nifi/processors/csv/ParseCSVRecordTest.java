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

import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.apache.nifi.processors.csv.AbstractCSVProcessor.DEFAULT_SCHEMA_ATTR_PREFIX;
import static org.apache.nifi.processors.csv.ParseCSVRecord.DEFAULT_VALUE_ATTR_PREFIX;
import static org.apache.nifi.processors.csv.ParseCSVRecord.PROP_RECORD_FROM_ATTRIBUTE;
import static org.apache.nifi.processors.csv.ParseCSVRecord.PROP_TRIM_VALUES;
import static org.apache.nifi.processors.csv.ParseCSVRecord.REL_FAILURE;
import static org.apache.nifi.processors.csv.ParseCSVRecord.REL_SUCCESS;


/**
 * Much of the actual CSV parsing logic has already been tested by {@link ExtractCSVHeaderTest}.
 */
public class ParseCSVRecordTest {

    @Before
    public void init() {
    }

    @Test
    public void defaultAttributesNoSchemaNoCustomHeader() {
        final TestRunner runner = TestRunners.newTestRunner(ParseCSVRecord.class);
        runner.enqueue("row1col1,row1col2\nrow2col1,row2col2");

        runner.run();

        runner.assertAllFlowFilesTransferred(REL_SUCCESS, 1);
        MockFlowFile ff = runner.getFlowFilesForRelationship(REL_SUCCESS).get(0);
        ff.assertContentEquals("row1col1,row1col2\nrow2col1,row2col2");
        ff.assertAttributeEquals(DEFAULT_VALUE_ATTR_PREFIX + "1", "row1col1");
        ff.assertAttributeEquals(DEFAULT_VALUE_ATTR_PREFIX + "2", "row1col2");
    }

    @Test
    public void defaultAttributesWithSchemaNoCustomHeader() {
        final TestRunner runner = TestRunners.newTestRunner(ParseCSVRecord.class);
        Map<String, String> attrs = new HashMap<>();
        attrs.put(DEFAULT_SCHEMA_ATTR_PREFIX + "1", "Column Name 1");
        attrs.put(DEFAULT_SCHEMA_ATTR_PREFIX + "2", "Column Name 2");
        runner.enqueue("row1col1,row1col2\nrow2col1,row2col2", attrs);

        runner.run();

        runner.assertAllFlowFilesTransferred(REL_SUCCESS, 1);
        MockFlowFile ff = runner.getFlowFilesForRelationship(REL_SUCCESS).get(0);
        ff.assertContentEquals("row1col1,row1col2\nrow2col1,row2col2");
        ff.assertAttributeEquals(DEFAULT_VALUE_ATTR_PREFIX + "Column Name 1", "row1col1");
        ff.assertAttributeEquals(DEFAULT_VALUE_ATTR_PREFIX + "Column Name 2", "row1col2");
        ff.assertAttributeNotExists(DEFAULT_VALUE_ATTR_PREFIX + "1");
        ff.assertAttributeNotExists(DEFAULT_VALUE_ATTR_PREFIX + "2");
    }

    @Test
    public void defaultAttributesTabsNoSchemaNoCustomHeader() {
        final TestRunner runner = TestRunners.newTestRunner(ParseCSVRecord.class);
        runner.setProperty(ParseCSVRecord.PROP_DELIMITER, "\t");

        runner.enqueue("row1col1\trow1col2\nrow2col1\trow2col2");

        runner.run();

        runner.assertAllFlowFilesTransferred(REL_SUCCESS, 1);
        MockFlowFile ff = runner.getFlowFilesForRelationship(REL_SUCCESS).get(0);
        ff.assertContentEquals("row1col1\trow1col2\nrow2col1\trow2col2");
        ff.assertAttributeEquals(DEFAULT_VALUE_ATTR_PREFIX + "1", "row1col1");
        ff.assertAttributeEquals(DEFAULT_VALUE_ATTR_PREFIX + "2", "row1col2");
    }


    @Test
    public void trimValues() {
        final TestRunner runner = TestRunners.newTestRunner(ParseCSVRecord.class);
        runner.setProperty(PROP_TRIM_VALUES, "true");
        runner.enqueue("row1col1,row1col2 \nrow2col1, row2col2");

        runner.run();

        runner.assertAllFlowFilesTransferred(REL_SUCCESS, 1);
        MockFlowFile ff = runner.getFlowFilesForRelationship(REL_SUCCESS).get(0);
        ff.assertContentEquals("row1col1,row1col2 \nrow2col1, row2col2");
        ff.assertAttributeEquals(DEFAULT_VALUE_ATTR_PREFIX + "1", "row1col1");
        ff.assertAttributeEquals(DEFAULT_VALUE_ATTR_PREFIX + "2", "row1col2");
    }

    @Test
    public void fromAttribute() {
        final TestRunner runner = TestRunners.newTestRunner(ParseCSVRecord.class);
        runner.setProperty(PROP_RECORD_FROM_ATTRIBUTE, "my.csv.record.in.attribute");
        Map<String, String> attrs = new HashMap<>();
        attrs.put("my.csv.record.in.attribute", "csvCol1,csvCol2");
        // test that content is not affected
        runner.enqueue("row1col1,row1col2\nrow2col1, row2col2", attrs);

        runner.run();

        runner.assertAllFlowFilesTransferred(REL_SUCCESS, 1);
        MockFlowFile ff = runner.getFlowFilesForRelationship(REL_SUCCESS).get(0);
        ff.assertContentEquals("row1col1,row1col2\nrow2col1, row2col2");
        ff.assertAttributeEquals(DEFAULT_VALUE_ATTR_PREFIX + "1", "csvCol1");
        ff.assertAttributeEquals(DEFAULT_VALUE_ATTR_PREFIX + "2", "csvCol2");
    }

    @Test
    public void nullInputFromAttribute() {
        final TestRunner runner = TestRunners.newTestRunner(ParseCSVRecord.class);
        runner.setProperty(PROP_RECORD_FROM_ATTRIBUTE, "my.csv.record.in.attribute");

        runner.enqueue("row1col1,row1col2\nrow2col1, row2col2");
        runner.run();

        runner.assertAllFlowFilesTransferred(REL_FAILURE);
    }
}
