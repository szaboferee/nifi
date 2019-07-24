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

import java.util.HashMap;
import java.util.Map;

import org.apache.nifi.util.MockFlowFile;
import org.junit.Before;
import org.junit.Test;

import okhttp3.mockwebserver.Dispatcher;
import okhttp3.mockwebserver.MockResponse;

public class ListSObjectMetaTest extends SalesForceProcessorTestBase {


  @Override
  @Before
  public void setUp() throws Exception {
    super.setUp();
    setupTestRunner(ListSObjectMeta.class);
  }

  @Test
  public void testNormalResponse() {
    testRunner.run();
    testRunner.assertTransferCount(ListSObjectMeta.REL_SUCCESS, 3);
    MockFlowFile flowFile = testRunner.getFlowFilesForRelationship(ListSObjectMeta.REL_SUCCESS).get(0);
    flowFile.assertAttributeEquals("salesforce.attributes.url", "/services/data/v46.0/sobjects/AcceptedEventRelation");
    flowFile.assertAttributeEquals("salesforce.attributes.objectName", "AcceptedEventRelation");
    flowFile.assertContentEquals("{\"activateable\":false,\"createable\":false,\"custom\":false,\"customSetting\":false,\"deletable\":false,\"deprecatedAndHidden\":false," +
      "\"feedEnabled\":false,\"hasSubtypes\":false,\"isSubtype\":false,\"keyPrefix\":null,\"label\":\"Accepted Event Relation\",\"labelPlural\":\"Accepted Event Relations\"," +
      "\"layoutable\":false,\"mergeable\":false,\"mruEnabled\":false,\"name\":\"AcceptedEventRelation\",\"queryable\":true,\"replicateable\":false,\"retrieveable\":true," +
      "\"searchable\":false,\"triggerable\":false,\"undeletable\":false,\"updateable\":false,\"urls\":{\"rowTemplate\":\"/services/data/v46.0/sobjects/AcceptedEventRelation/{ID}\"," +
      "\"defaultValues\":\"/services/data/v46.0/sobjects/AcceptedEventRelation/defaultValues?recordTypeId&fields\",\"describe\":\"/services/data/v46.0/sobjects/AcceptedEventRelation/describe\"," +
      "\"sobject\":\"/services/data/v46.0/sobjects/AcceptedEventRelation\"}}");

  }

  @Override
  protected Dispatcher getDispatcher() {
    Map<String, MockResponse> mockResponseMap = new HashMap<>();
    mockResponseMap.put("/services/data/v46.0/sobjects", createMockResponse("fixtures/describe_global.json", 200));
    return getDispatcher(mockResponseMap);
  }
}