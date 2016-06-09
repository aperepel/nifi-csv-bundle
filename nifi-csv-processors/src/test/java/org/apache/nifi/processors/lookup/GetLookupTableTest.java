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
package org.apache.nifi.processors.lookup;

import org.apache.nifi.lookup.InMemoryLookupTableService;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.nifi.processors.lookup.GetLookupTable.PROP_LOOKUP_ENTRY_ID;
import static org.apache.nifi.processors.lookup.GetLookupTable.PROP_LOOKUP_TABLE_SERVICE;
import static org.apache.nifi.processors.lookup.GetLookupTable.PROP_PUT_CACHE_VALUE_IN_ATTRIBUTE;
import static org.apache.nifi.processors.lookup.GetLookupTable.REL_FOUND;
import static org.apache.nifi.processors.lookup.GetLookupTable.REL_NOT_FOUND;

public class GetLookupTableTest {

    private TestRunner runner;
    private InMemoryLookupTableService service;

    @Before
    public void init() throws InitializationException {
        runner = TestRunners.newTestRunner(GetLookupTable.class);
        service = new InMemoryLookupTableService();
        runner.addControllerService("service id", service);
        runner.enableControllerService(service);
        runner.assertValid(service);

        runner.setProperty(PROP_LOOKUP_TABLE_SERVICE, "service id");
    }

    @Test
    public void notFound() throws Exception {
        runner.setProperty(PROP_LOOKUP_ENTRY_ID, "${key}");
        Map<String, String> attrs = new HashMap<>();
        attrs.put("key", "lookupKey1");
        attrs.put("do not touch this", "property");
        runner.enqueue("payload1", attrs);
        runner.run();

        runner.assertAllFlowFilesTransferred(REL_NOT_FOUND);
        runner.assertTransferCount(REL_NOT_FOUND, 1);
        List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(REL_NOT_FOUND);
        MockFlowFile ff = flowFiles.get(0);
        ff.assertContentEquals("payload1");
        ff.assertAttributeEquals("do not touch this", "property");
    }

    @Test
    public void foundIntoPayload() throws Exception {
        runner.setProperty(PROP_LOOKUP_ENTRY_ID, "${key}");
        Map<String, String> attrs = new HashMap<>();
        attrs.put("key", "lookupKey1");
        attrs.put("do not touch this", "property");

        service.put("lookupKey1", "looked up value");
        runner.enqueue("payload1", attrs);
        runner.run();

        runner.assertAllFlowFilesTransferred(REL_FOUND);
        runner.assertTransferCount(REL_FOUND, 1);
        List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(REL_FOUND);
        MockFlowFile ff = flowFiles.get(0);
        ff.assertContentEquals("looked up value");
        ff.assertAttributeEquals("do not touch this", "property");
    }

    @Test
    public void foundIntoAttribute() throws Exception {
        runner.setProperty(PROP_LOOKUP_ENTRY_ID, "${key}");
        runner.setProperty(PROP_PUT_CACHE_VALUE_IN_ATTRIBUTE, "attr for lookup results");
        Map<String, String> attrs = new HashMap<>();
        attrs.put("key", "lookupKey1");
        attrs.put("attr for lookup results", "not found");

        service.put("lookupKey1", "looked up value");
        runner.enqueue("payload1", attrs);
        runner.run();

        runner.assertAllFlowFilesTransferred(REL_FOUND);
        runner.assertTransferCount(REL_FOUND, 1);
        List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(REL_FOUND);
        MockFlowFile ff = flowFiles.get(0);
        ff.assertContentEquals("payload1");
        ff.assertAttributeEquals("attr for lookup results", "looked up value");
    }

}
