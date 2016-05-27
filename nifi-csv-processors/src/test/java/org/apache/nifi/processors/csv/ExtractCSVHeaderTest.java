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

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.List;

import static org.apache.nifi.processors.csv.ExtractCSVHeader.ATTR_HEADER_COLUMN_COUNT;
import static org.apache.nifi.processors.csv.ExtractCSVHeader.ATTR_HEADER_ORIGINAL;
import static org.apache.nifi.processors.csv.ExtractCSVHeader.DEFAULT_SCHEMA_ATTR_PREFIX;
import static org.apache.nifi.processors.csv.ExtractCSVHeader.PROP_SCHEMA_ATTR_PREFIX;
import static org.apache.nifi.processors.csv.ExtractCSVHeader.REL_CONTENT;
import static org.apache.nifi.processors.csv.ExtractCSVHeader.REL_ORIGINAL;


public class ExtractCSVHeaderTest {

    final Path dataPath = Paths.get("src/test/resources/ExtractCSVHeaderTest");


    @Before
    public void init() {
    }

    @Test
    public void cleanHeader() throws IOException {
        final TestRunner runner = TestRunners.newTestRunner(ExtractCSVHeader.class);
        final Path file = dataPath.resolve("test1.csv");

        runner.enqueue(file);
        runner.run();
        runner.assertTransferCount(REL_ORIGINAL, 1);
        List<MockFlowFile> output = runner.getFlowFilesForRelationship(REL_ORIGINAL);
        MockFlowFile ff = output.get(0);
        ff.assertAttributeEquals(DEFAULT_SCHEMA_ATTR_PREFIX + ATTR_HEADER_ORIGINAL,
                "Registry,Assignment,Organization Name,Organization Address");
        ff.assertAttributeEquals(DEFAULT_SCHEMA_ATTR_PREFIX + ATTR_HEADER_COLUMN_COUNT, "4");
        ff.assertAttributeEquals(DEFAULT_SCHEMA_ATTR_PREFIX + "1", "Registry");
        ff.assertAttributeEquals(DEFAULT_SCHEMA_ATTR_PREFIX + "2", "Assignment");
        ff.assertAttributeEquals(DEFAULT_SCHEMA_ATTR_PREFIX + "3", "Organization Name");
        ff.assertAttributeEquals(DEFAULT_SCHEMA_ATTR_PREFIX + "4", "Organization Address");
    }

    @Test
    public void attributePrefix() throws IOException {
        final TestRunner runner = TestRunners.newTestRunner(ExtractCSVHeader.class);
        final Path file = dataPath.resolve("test1.csv");
        String prefix = "my.prefix.column";
        runner.setProperty(PROP_SCHEMA_ATTR_PREFIX, prefix);

        runner.enqueue(file);
        runner.run();
        runner.assertTransferCount(REL_ORIGINAL, 1);
        List<MockFlowFile> output = runner.getFlowFilesForRelationship(REL_ORIGINAL);
        MockFlowFile ff = output.get(0);
        ff.assertAttributeEquals(prefix + ATTR_HEADER_ORIGINAL,
                "Registry,Assignment,Organization Name,Organization Address");
        ff.assertAttributeEquals(prefix + ATTR_HEADER_COLUMN_COUNT, "4");
        ff.assertAttributeEquals(prefix + "1", "Registry");
        ff.assertAttributeEquals(prefix + "2", "Assignment");
        ff.assertAttributeEquals(prefix + "3", "Organization Name");
        ff.assertAttributeEquals(prefix + "4", "Organization Address");
    }

    @Test
    public void mixedHeader() throws IOException {
        final TestRunner runner = TestRunners.newTestRunner(ExtractCSVHeader.class);
        final Path file = dataPath.resolve("test2.csv");

        runner.enqueue(file);
        runner.run();
        runner.assertTransferCount(REL_ORIGINAL, 1);
        List<MockFlowFile> output = runner.getFlowFilesForRelationship(REL_ORIGINAL);
        MockFlowFile ff = output.get(0);
        ff.assertAttributeEquals(DEFAULT_SCHEMA_ATTR_PREFIX + ATTR_HEADER_ORIGINAL,
                "\"Registry Name\",Assignment,\"Organization Name & Notes\",\"Organization Address, and Stuff\"");
        ff.assertAttributeEquals(DEFAULT_SCHEMA_ATTR_PREFIX + ATTR_HEADER_COLUMN_COUNT, "4");
        ff.assertAttributeEquals(DEFAULT_SCHEMA_ATTR_PREFIX + "1", "Registry Name");
        ff.assertAttributeEquals(DEFAULT_SCHEMA_ATTR_PREFIX + "2", "Assignment");
        ff.assertAttributeEquals(DEFAULT_SCHEMA_ATTR_PREFIX + "3", "Organization Name & Notes");
        ff.assertAttributeEquals(DEFAULT_SCHEMA_ATTR_PREFIX + "4", "Organization Address, and Stuff");
    }

    @Test
    public void cleanHeaderTabs() throws IOException {
        final TestRunner runner = TestRunners.newTestRunner(ExtractCSVHeader.class);
        runner.setProperty(ExtractCSVHeader.PROP_FORMAT, ExtractCSVHeader.VALUE_TAB);
        final Path file = dataPath.resolve("test1tabs.csv");

        runner.enqueue(file);
        runner.run();
        runner.assertTransferCount(REL_ORIGINAL, 1);
        List<MockFlowFile> output = runner.getFlowFilesForRelationship(REL_ORIGINAL);
        MockFlowFile ff = output.get(0);
        ff.assertAttributeEquals(DEFAULT_SCHEMA_ATTR_PREFIX + ATTR_HEADER_ORIGINAL,
                "Registry\tAssignment\tOrganization Name\tOrganization Address");
        ff.assertAttributeEquals(DEFAULT_SCHEMA_ATTR_PREFIX + ATTR_HEADER_COLUMN_COUNT, "4");
        ff.assertAttributeEquals(DEFAULT_SCHEMA_ATTR_PREFIX + "1", "Registry");
        ff.assertAttributeEquals(DEFAULT_SCHEMA_ATTR_PREFIX + "2", "Assignment");
        ff.assertAttributeEquals(DEFAULT_SCHEMA_ATTR_PREFIX + "3", "Organization Name");
        ff.assertAttributeEquals(DEFAULT_SCHEMA_ATTR_PREFIX + "4", "Organization Address");
    }

    @Test
    public void cleanHeaderExcelTabs() throws IOException {
        final TestRunner runner = TestRunners.newTestRunner(ExtractCSVHeader.class);
        runner.setProperty(ExtractCSVHeader.PROP_FORMAT, ExtractCSVHeader.VALUE_EXCEL);
        runner.setProperty(ExtractCSVHeader.PROP_DELIMITER, "\t");
        final Path file = dataPath.resolve("test1tabs.csv");

        runner.enqueue(file);
        runner.run();
        runner.assertTransferCount(REL_ORIGINAL, 1);
        List<MockFlowFile> output = runner.getFlowFilesForRelationship(REL_ORIGINAL);
        MockFlowFile ff = output.get(0);
        ff.assertAttributeEquals(DEFAULT_SCHEMA_ATTR_PREFIX + ATTR_HEADER_ORIGINAL,
                "Registry\tAssignment\tOrganization Name\tOrganization Address");
        ff.assertAttributeEquals(DEFAULT_SCHEMA_ATTR_PREFIX + ATTR_HEADER_COLUMN_COUNT, "4");
        ff.assertAttributeEquals(DEFAULT_SCHEMA_ATTR_PREFIX + "1", "Registry");
        ff.assertAttributeEquals(DEFAULT_SCHEMA_ATTR_PREFIX + "2", "Assignment");
        ff.assertAttributeEquals(DEFAULT_SCHEMA_ATTR_PREFIX + "3", "Organization Name");
        ff.assertAttributeEquals(DEFAULT_SCHEMA_ATTR_PREFIX + "4", "Organization Address");
    }

    @Test
    public void unsupportedFormat() throws IOException {
        final TestRunner runner = TestRunners.newTestRunner(ExtractCSVHeader.class);
        runner.setProperty(ExtractCSVHeader.PROP_FORMAT, "NO_SUCH_FORMAT");
        runner.setProperty(ExtractCSVHeader.PROP_DELIMITER, "\t");
        final Path file = dataPath.resolve("test1tabs.csv");

        runner.enqueue(file);
        runner.assertNotValid();
    }

    @Test
    public void formatFromExpression() throws IOException {
        final TestRunner runner = TestRunners.newTestRunner(ExtractCSVHeader.class);
        final Path file = dataPath.resolve("test1tabs.csv");

        // would need special arrangements for property, EL and validator to support this
        // runner.setProperty(ExtractCSVHeader.PROP_FORMAT, "${my.incoming.format}");
        // runner.enqueue(file, Collections.singletonMap("my.incoming.format", "TAB"));
        runner.setProperty(ExtractCSVHeader.PROP_DELIMITER, "${my.incoming.delimiter}");
        runner.enqueue(file, Collections.singletonMap("my.incoming.delimiter", "\t"));
        runner.run();
        runner.assertTransferCount(REL_ORIGINAL, 1);
        List<MockFlowFile> output = runner.getFlowFilesForRelationship(REL_ORIGINAL);
        MockFlowFile ff = output.get(0);
        ff.assertAttributeEquals(DEFAULT_SCHEMA_ATTR_PREFIX + ATTR_HEADER_ORIGINAL,
                "Registry\tAssignment\tOrganization Name\tOrganization Address");
        ff.assertAttributeEquals(DEFAULT_SCHEMA_ATTR_PREFIX + ATTR_HEADER_COLUMN_COUNT, "4");
        ff.assertAttributeEquals(DEFAULT_SCHEMA_ATTR_PREFIX + "1", "Registry");
        ff.assertAttributeEquals(DEFAULT_SCHEMA_ATTR_PREFIX + "2", "Assignment");
        ff.assertAttributeEquals(DEFAULT_SCHEMA_ATTR_PREFIX + "3", "Organization Name");
        ff.assertAttributeEquals(DEFAULT_SCHEMA_ATTR_PREFIX + "4", "Organization Address");
    }

    @Test
    public void multiByteDelimiter() {
        final TestRunner runner = TestRunners.newTestRunner(ExtractCSVHeader.class);
        runner.setProperty(ExtractCSVHeader.PROP_DELIMITER, ":::");
        runner.enqueue("test");
        runner.assertNotValid();
    }

    @Test
    public void contentRelationship() {
        final TestRunner runner = TestRunners.newTestRunner(ExtractCSVHeader.class);
        runner.enqueue("header1,header2\nrow1col1,row1col2\nrow2col1,row2col2");

        runner.run();

        runner.assertTransferCount(REL_ORIGINAL, 1);
        runner.assertTransferCount(REL_CONTENT, 1);
        MockFlowFile ff = runner.getFlowFilesForRelationship(REL_CONTENT).get(0);
        ff.assertContentEquals("row1col1,row1col2\nrow2col1,row2col2");
        ff.assertAttributeEquals(DEFAULT_SCHEMA_ATTR_PREFIX + ATTR_HEADER_COLUMN_COUNT, "2");
    }
}
