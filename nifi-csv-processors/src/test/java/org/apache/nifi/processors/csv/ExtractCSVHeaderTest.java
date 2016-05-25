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
import java.util.List;

import static org.apache.nifi.processors.csv.ExtractCSVHeader.ATTR_HEADER_COLUMN_COUNT;
import static org.apache.nifi.processors.csv.ExtractCSVHeader.ATTR_HEADER_COLUMN_PREFIX;
import static org.apache.nifi.processors.csv.ExtractCSVHeader.ATTR_HEADER_ORIGINAL;
import static org.apache.nifi.processors.csv.ExtractCSVHeader.REL_SUCCESS;


public class ExtractCSVHeaderTest {

    final Path dataPath = Paths.get("src/test/resources/ExtractCSVHeaderTest");


    @Before
    public void init() {
    }

    @Test
    public void testCleanHeader() throws IOException {
        final TestRunner runner = TestRunners.newTestRunner(ExtractCSVHeader.class);
        final Path file = dataPath.resolve("test1.csv");

        runner.enqueue(file);
        runner.run();
        runner.assertAllFlowFilesTransferred(REL_SUCCESS);
        runner.assertTransferCount(REL_SUCCESS, 1);
        List<MockFlowFile> output = runner.getFlowFilesForRelationship(REL_SUCCESS);
        MockFlowFile ff = output.get(0);
        ff.assertAttributeEquals(ATTR_HEADER_ORIGINAL,
                "Registry,Assignment,Organization Name,Organization Address");
        ff.assertAttributeEquals(ATTR_HEADER_COLUMN_COUNT, "4");
        ff.assertAttributeEquals(ATTR_HEADER_COLUMN_PREFIX + "1", "Registry");
        ff.assertAttributeEquals(ATTR_HEADER_COLUMN_PREFIX + "2", "Assignment");
        ff.assertAttributeEquals(ATTR_HEADER_COLUMN_PREFIX + "3", "Organization Name");
        ff.assertAttributeEquals(ATTR_HEADER_COLUMN_PREFIX + "4", "Organization Address");
    }

    @Test
    public void testMixedHeader() throws IOException {
        final TestRunner runner = TestRunners.newTestRunner(ExtractCSVHeader.class);
        final Path file = dataPath.resolve("test2.csv");

        runner.enqueue(file);
        runner.run();
        runner.assertAllFlowFilesTransferred(REL_SUCCESS);
        runner.assertTransferCount(REL_SUCCESS, 1);
        List<MockFlowFile> output = runner.getFlowFilesForRelationship(REL_SUCCESS);
        MockFlowFile ff = output.get(0);
        ff.assertAttributeEquals(ATTR_HEADER_ORIGINAL,
                "\"Registry Name\",Assignment,\"Organization Name & Notes\",\"Organization Address, and Stuff\"");
        ff.assertAttributeEquals(ATTR_HEADER_COLUMN_COUNT, "4");
        ff.assertAttributeEquals(ATTR_HEADER_COLUMN_PREFIX + "1", "Registry Name");
        ff.assertAttributeEquals(ATTR_HEADER_COLUMN_PREFIX + "2", "Assignment");
        ff.assertAttributeEquals(ATTR_HEADER_COLUMN_PREFIX + "3", "Organization Name & Notes");
        ff.assertAttributeEquals(ATTR_HEADER_COLUMN_PREFIX + "4", "Organization Address, and Stuff");
    }


}
