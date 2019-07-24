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

import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.when;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.nifi.util.MockFlowFile;
import org.junit.Before;
import org.junit.Test;

import okhttp3.mockwebserver.Dispatcher;
import okhttp3.mockwebserver.MockResponse;


public class ListSOQLTest extends SalesForceProcessorTestBase {

  @Override
  @Before
  public void setUp() throws Exception {
    super.setUp();
    setupTestRunner(ListSOQL.class);
  }


  @Test
  public void testSimpleResponseNoInput() {
    testRunner.setProperty(ListSOQL.QUERY, "SELECT Name FROM Account");
    testRunner.setIncomingConnection(false);
    testRunner.run();
    testRunner.assertTransferCount(ListSOQL.REL_SUCCESS, 12);

    MockFlowFile flowFile = testRunner.getFlowFilesForRelationship(ListSOQL.REL_SUCCESS).get(0);
    flowFile.assertAttributeEquals(
      "salesforce.attributes.type", "Account");
    flowFile.assertAttributeEquals(
      "salesforce.attributes.url",
      "/services/data/v46.0/sobjects/Account/0012p00002OV0hJAAT");
    flowFile.assertAttributeEquals(
      "salesforce.attributes.someNumber", "10");
    flowFile.assertAttributeEquals(
      "salesforce.attributes.someArray", "[1,2]");
    flowFile.assertAttributeEquals(
      "salesforce.attributes.someObject", "{\"key\":\"value\"}");

    flowFile.assertContentEquals("{\"attributes\":{\"type\":\"Account\",\"url\":\"/services/data/v46.0/sobjects/Account/0012p00002OV0hJAAT\"," +
      "\"someNumber\":10,\"someArray\":[1,2],\"someObject\":{\"key\":\"value\"}},\"Name\":\"GenePoint\"}");
  }

  @Test
  public void testSimpleResponseWithInput() {
    HashMap<String, String> attributes = new HashMap<>();
    attributes.put("objectName", "Account");
    testRunner.enqueue("", attributes);
    testRunner.setProperty(ListSOQL.QUERY, "SELECT Name FROM ${objectName}");

    testRunner.run();
    testRunner.assertTransferCount(ListSOQL.REL_SUCCESS, 12);

    MockFlowFile flowFile = testRunner.getFlowFilesForRelationship(ListSOQL.REL_SUCCESS).get(0);
    flowFile.assertAttributeEquals(
      "salesforce.attributes.type", "Account");
    flowFile.assertAttributeEquals(
      "salesforce.attributes.url",
      "/services/data/v46.0/sobjects/Account/0012p00002OV0hJAAT");
    flowFile.assertAttributeEquals(
      "salesforce.attributes.someNumber", "10");
    flowFile.assertAttributeEquals(
      "salesforce.attributes.someArray", "[1,2]");
    flowFile.assertAttributeEquals(
      "salesforce.attributes.someObject", "{\"key\":\"value\"}");

    flowFile.assertContentEquals("{\"attributes\":{\"type\":\"Account\",\"url\":\"/services/data/v46.0/sobjects/Account/0012p00002OV0hJAAT\"," +
      "\"someNumber\":10,\"someArray\":[1,2],\"someObject\":{\"key\":\"value\"}},\"Name\":\"GenePoint\"}");
  }

  @Test
  public void testMultiPartResponse() {
    testRunner.setProperty(ListSOQL.QUERY, "SELECT Id FROM Account");
    testRunner.setIncomingConnection(false);

    testRunner.run();
    testRunner.assertTransferCount(ListSOQL.REL_SUCCESS, 12);
  }

  @Test
  public void testSessionRenewalResponse() {

    AtomicReference<String> token = new AtomicReference<>("invalidToken");
    when(mockAuthService.getToken()).thenAnswer(invocationOnMock -> token.get());
    doAnswer(invocationOnMock -> {
      token.set("validToken");
      return null;
    }).when(mockAuthService).renew();
    testRunner.setIncomingConnection(false);
    testRunner.setProperty(ListSOQL.QUERY, "SELECT Id FROM Account");

    testRunner.run();
    testRunner.assertTransferCount(ListSOQL.REL_SUCCESS, 12);
  }

  @Override
  protected Dispatcher getDispatcher() {
    Map<String, MockResponse> mockResponseMap = new HashMap<>();
    mockResponseMap.put(
      "/services/data/v46.0/query?q=SELECT%20Name%20FROM%20Account",
      createMockResponse("fixtures/soql_single.json", 200));
    mockResponseMap.put(
      "/services/data/v46.0/query?q=SELECT%20Id%20FROM%20Account",
      createMockResponse("fixtures/soql_multi_1.json", 200));
    mockResponseMap.put(
      "/services/data/v46.0/query/nextQueryId",
      createMockResponse("fixtures/soql_multi_2.json", 200));

    return getDispatcher(mockResponseMap);

  }
}