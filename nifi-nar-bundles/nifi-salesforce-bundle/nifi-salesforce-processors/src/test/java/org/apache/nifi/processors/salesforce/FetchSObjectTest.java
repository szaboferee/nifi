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
package org.apache.nifi.processors.salesforce;

import java.io.IOException;
import java.util.HashMap;

import org.apache.nifi.util.MockFlowFile;
import org.junit.Before;
import org.junit.Test;

import okhttp3.mockwebserver.Dispatcher;
import okhttp3.mockwebserver.MockResponse;

public class FetchSObjectTest extends SalesForceProcessorTestBase {

  @Override
  @Before
  public void setUp() throws Exception {
    super.setUp();
    setupTestRunner(FetchSObject.class);
  }

  @Test
  public void testFetchObject() throws IOException {

    HashMap<String, String> attributes = new HashMap<>();
    attributes.put("salesforce.attributes.url", "/services/data/v46.0/sobjects/Account/0012p00002NlZRyAAN");
    attributes.put("salesforce.attributes.type",  "Account");
    testRunner.enqueue("", attributes);

    testRunner.run();

    testRunner.assertTransferCount(FetchSObject.REL_SUCCESS, 1);
    MockFlowFile flowFile = testRunner.getFlowFilesForRelationship(FetchSObject.REL_SUCCESS).get(0);
    flowFile.assertContentEquals(readFile("fixtures/sobject.json"));
    flowFile.assertAttributeEquals("salesforce.attributes.url", "/services/data/v46.0/sobjects/Account/0012p00002NlZRyAAN");
    flowFile.assertAttributeEquals("salesforce.attributes.type",  "Account");

  }


  @Override
  protected Dispatcher getDispatcher() {
    HashMap<String, MockResponse> mockResponseMap = new HashMap<>();
    mockResponseMap.put("/services/data/v46.0/sobjects/Account/0012p00002NlZRyAAN", createMockResponse("fixtures/sobject.json", 200));

    return getDispatcher(mockResponseMap);
  }
}