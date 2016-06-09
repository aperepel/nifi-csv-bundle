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

import static org.apache.nifi.processors.lookup.PutLookupTable.PROP_LOOKUP_ENTRY_ID;
import static org.apache.nifi.processors.lookup.PutLookupTable.PROP_LOOKUP_TABLE_SERVICE;
import static org.apache.nifi.processors.lookup.PutLookupTable.PROP_LOOKUP_TABLE_UPDATE_STRATEGY;
import static org.apache.nifi.processors.lookup.PutLookupTable.REL_FAILURE;
import static org.apache.nifi.processors.lookup.PutLookupTable.REL_SUCCESS;
import static org.apache.nifi.processors.lookup.PutLookupTable.STRATEGY_KEEP_ORIGINAL;


public class PutLookupTableTest {

    private TestRunner runner;

    @Before
    public void init() throws InitializationException {
        runner = TestRunners.newTestRunner(PutLookupTable.class);
        final InMemoryLookupTableService service = new InMemoryLookupTableService();
        runner.addControllerService("service id", service);
        runner.enableControllerService(service);
        runner.assertValid(service);

        runner.setProperty(PROP_LOOKUP_TABLE_SERVICE, "service id");
    }

    @Test
    public void repeatablePutDefaults() throws Exception {
        runner.setProperty(PROP_LOOKUP_ENTRY_ID, "${key}");
        Map<String, String> attrs = new HashMap<>();
        attrs.put("key", "lookupKey1");
        runner.enqueue("payload1", attrs);
        runner.run();

        runner.assertAllFlowFilesTransferred(REL_SUCCESS);
        runner.assertTransferCount(REL_SUCCESS, 1);
        List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(REL_SUCCESS);
        MockFlowFile ff = flowFiles.get(0);
        ff.assertContentEquals("payload1");

        runner.clearTransferState();

        runner.enqueue("payload2", attrs);
        runner.run();
        runner.assertAllFlowFilesTransferred(REL_SUCCESS);
        runner.assertTransferCount(REL_SUCCESS, 1);
        flowFiles = runner.getFlowFilesForRelationship(REL_SUCCESS);
        ff = flowFiles.get(0);
        ff.assertContentEquals("payload2");
    }

    @Test
    public void repeatablePutKeepOriginal() throws Exception {
        runner.setProperty(PROP_LOOKUP_ENTRY_ID, "${key}");
        runner.setProperty(PROP_LOOKUP_TABLE_UPDATE_STRATEGY, STRATEGY_KEEP_ORIGINAL);
        Map<String, String> attrs = new HashMap<>();
        attrs.put("key", "lookupKey1");
        runner.enqueue("payload1", attrs);
        runner.run();

        runner.assertAllFlowFilesTransferred(REL_SUCCESS);
        runner.assertTransferCount(REL_SUCCESS, 1);
        List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(REL_SUCCESS);
        MockFlowFile ff = flowFiles.get(0);
        ff.assertContentEquals("payload1");

        runner.enqueue("payload2", attrs);
        runner.run();
        runner.assertTransferCount(REL_SUCCESS, 1);
        runner.assertTransferCount(REL_FAILURE, 1);
        flowFiles = runner.getFlowFilesForRelationship(REL_FAILURE);
        ff = flowFiles.get(0);
        ff.assertContentEquals("payload2");
    }

}
