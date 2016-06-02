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

import org.apache.nifi.processors.standard.RegexAttributesToJSON;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.apache.nifi.processors.standard.RegexAttributesToJSON.ATTRIBUTES_REGEX;
import static org.apache.nifi.processors.standard.RegexAttributesToJSON.ATTRIBUTES_STRIP_PREFIX;
import static org.apache.nifi.processors.standard.RegexAttributesToJSON.DESTINATION;
import static org.apache.nifi.processors.standard.RegexAttributesToJSON.DESTINATION_CONTENT;
import static org.apache.nifi.processors.standard.RegexAttributesToJSON.INCLUDE_CORE_ATTRIBUTES;
import static org.apache.nifi.processors.standard.RegexAttributesToJSON.REL_SUCCESS;


public class RegexAttributesToJSONTest {

    @Before
    public void init() {
    }

    @Test
    public void regexAttributesFilter() throws IOException {
        final TestRunner runner = TestRunners.newTestRunner(RegexAttributesToJSON.class);
        runner.setProperty(ATTRIBUTES_REGEX, "my.prefix.\\d+");
        runner.setProperty(INCLUDE_CORE_ATTRIBUTES, "false");
        runner.setProperty(DESTINATION, DESTINATION_CONTENT);

        Map<String, String> attrs = new HashMap<>();
        attrs.put("my.prefix.1", "value1");
        attrs.put("my.prefix.2", "value2");
        attrs.put("my.excluded.prefix.3", "value3");

        runner.enqueue("will be replaced", attrs);
        runner.run();
        runner.assertAllFlowFilesTransferred(REL_SUCCESS);
        runner.assertTransferCount(REL_SUCCESS, 1);
        List<MockFlowFile> output = runner.getFlowFilesForRelationship(REL_SUCCESS);
        MockFlowFile ff = output.get(0);
        ff.assertContentEquals("{\"my.prefix.1\":\"value1\",\"my.prefix.2\":\"value2\"}");
        ff.assertAttributeEquals("my.excluded.prefix.3", "value3");
    }

    @Test
    public void stripPrefix() {
        final TestRunner runner = TestRunners.newTestRunner(RegexAttributesToJSON.class);
        runner.setProperty(ATTRIBUTES_REGEX, "my.prefix..*");
        runner.setProperty(INCLUDE_CORE_ATTRIBUTES, "false");
        runner.setProperty(DESTINATION, DESTINATION_CONTENT);
        runner.setProperty(ATTRIBUTES_STRIP_PREFIX, "my.prefix.");

        Map<String, String> attrs = new HashMap<>();
        attrs.put("my.prefix.column1", "value1");
        attrs.put("my.prefix.column2", "value2");
        attrs.put("my.excluded.prefix.3", "value3");

        runner.enqueue("will be replaced", attrs);
        runner.run();
        runner.assertAllFlowFilesTransferred(REL_SUCCESS);
        runner.assertTransferCount(REL_SUCCESS, 1);
        List<MockFlowFile> output = runner.getFlowFilesForRelationship(REL_SUCCESS);
        MockFlowFile ff = output.get(0);
        ff.assertContentEquals("{\"column1\":\"value1\",\"column2\":\"value2\"}");
        ff.assertAttributeEquals("my.excluded.prefix.3", "value3");
    }

    @Test
    public void stripNonExistentPrefix() {
        final TestRunner runner = TestRunners.newTestRunner(RegexAttributesToJSON.class);
        runner.setProperty(ATTRIBUTES_REGEX, "my.prefix..*");
        runner.setProperty(INCLUDE_CORE_ATTRIBUTES, "false");
        runner.setProperty(DESTINATION, DESTINATION_CONTENT);
        runner.setProperty(ATTRIBUTES_STRIP_PREFIX, "no.such.prefix.");

        Map<String, String> attrs = new LinkedHashMap<>();
        attrs.put("my.prefix.column1", "value1");
        attrs.put("my.prefix.column2", "value2");
        attrs.put("my.excluded.prefix.3", "value3");

        runner.enqueue("will be replaced", attrs);
        runner.run();
        runner.assertAllFlowFilesTransferred(REL_SUCCESS);
        runner.assertTransferCount(REL_SUCCESS, 1);
        List<MockFlowFile> output = runner.getFlowFilesForRelationship(REL_SUCCESS);
        MockFlowFile ff = output.get(0);
        ff.assertContentEquals("{\"my.prefix.column2\":\"value2\",\"my.prefix.column1\":\"value1\"}");
        ff.assertAttributeEquals("my.excluded.prefix.3", "value3");
    }

}