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

import java.io.IOException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.nifi.components.state.Scope;
import org.apache.nifi.components.state.StateMap;
import org.apache.nifi.util.MockFlowFile;
import org.junit.Before;
import org.junit.Test;

import okhttp3.mockwebserver.Dispatcher;
import okhttp3.mockwebserver.MockResponse;


public class
QuerySOQLTest extends SalesForceProcessorTestBase {

  @Override
  @Before
  public void setUp() throws Exception {
    super.setUp();
    setupTestRunner(QuerySOQL.class);
  }

  @Test
  public void testSimpleResponseDefaultValues() throws IOException {
    testRunner.setProperty(QuerySOQL.OBJECT_NAME, "Account");
    testRunner.setProperty(QuerySOQL.FIELD_NAMES, "Name");
    testRunner.run();
    testRunner.assertTransferCount(QuerySOQL.REL_SUCCESS, 1);

    assertResult("fixtures/flowfile/query_no_limit.json");
  }

  private void assertResult(String fileName) throws IOException {
    MockFlowFile flowFile = testRunner.getFlowFilesForRelationship(QuerySOQL.REL_SUCCESS).get(0);
    assertFlowfileContent(fileName, flowFile);
  }

  private void assertFlowfileContent(String fileName, MockFlowFile flowFile) throws IOException {
    flowFile.assertAttributeEquals(
        "salesforce.attributes.type", "Account");

    URL resource = getClass().getClassLoader().getResource(fileName);
    byte[] content = Files.readAllBytes(Paths.get(resource.getPath()));
    flowFile.assertContentEquals(new String(content));
  }

  @Test
  public void testMultiPartResponse() throws IOException {
    testRunner.setProperty(QuerySOQL.OBJECT_NAME, "Account");
    testRunner.setProperty(QuerySOQL.FIELD_NAMES, "Id");
    testRunner.run();
    testRunner.assertTransferCount(QuerySOQL.REL_SUCCESS, 1);
    assertResult("fixtures/flowfile/query_no_limit.json");

  }

  @Test
  public void testQueryWithLimit() throws IOException {
    testRunner.setProperty(QuerySOQL.OBJECT_NAME, "Account");
    testRunner.setProperty(QuerySOQL.FIELD_NAMES, "Id");
    testRunner.setProperty(QuerySOQL.MAX_VALUE_COLUMN_NAMES, "Id");
    testRunner.setProperty(QuerySOQL.MAX_ROWS_PER_FLOW_FILE, "3");
    testRunner.setProperty("initial.maxvalue.Id", "0000000000000000");

    testRunner.run(2);
    testRunner.assertTransferCount(QuerySOQL.REL_SUCCESS, 4);
    List<MockFlowFile> flowFiles = testRunner.getFlowFilesForRelationship(QuerySOQL.REL_SUCCESS);
    assertFlowfileContent("fixtures/flowfile/query_limit_3_1.json", flowFiles.get(0));
    assertFlowfileContent("fixtures/flowfile/query_limit_3_2.json", flowFiles.get(1));
    assertFlowfileContent("fixtures/flowfile/query_limit_3_3.json", flowFiles.get(2));
    assertFlowfileContent("fixtures/flowfile/query_limit_3_4.json", flowFiles.get(3));
  }

  @Test
  public void testQueryWithMaxColumnAndInitialValue() throws IOException {
    testRunner.setProperty(QuerySOQL.OBJECT_NAME, "Account");
    testRunner.setProperty(QuerySOQL.FIELD_NAMES, "Id");
    testRunner.setProperty(QuerySOQL.MAX_VALUE_COLUMN_NAMES, "Id");
    testRunner.setProperty("initial.maxvalue.Id", "0000000000000000");
    testRunner.run();
    testRunner.assertTransferCount(QuerySOQL.REL_SUCCESS, 1);
    assertResult("fixtures/flowfile/query_no_limit_id.json");
    StateMap state = testRunner.getStateManager().getState(Scope.CLUSTER);
    testRunner.getStateManager().assertStateEquals("account@!@id", "0012p00002OV0hKAAT", Scope.CLUSTER);
  }

  @Test
  public void testQueryWithMaxColumnWithoutInitialValue() throws IOException {
    testRunner.setProperty(QuerySOQL.OBJECT_NAME, "Account");
    testRunner.setProperty(QuerySOQL.FIELD_NAMES, "Id");
    testRunner.setProperty(QuerySOQL.MAX_VALUE_COLUMN_NAMES, "Id");
    testRunner.run(3);
    testRunner.assertTransferCount(QuerySOQL.REL_SUCCESS, 2);

    List<MockFlowFile> flowFiles = testRunner.getFlowFilesForRelationship(QuerySOQL.REL_SUCCESS);

    assertFlowfileContent("fixtures/flowfile/query_no_limit_id_no_initial_value_1.json", flowFiles.get(0));
    assertFlowfileContent("fixtures/flowfile/query_no_limit_id_no_initial_value_2.json", flowFiles.get(1));

    StateMap state = testRunner.getStateManager().getState(Scope.CLUSTER);
    testRunner.getStateManager().assertStateEquals("account@!@id", "0012p00002OV0hKAAT", Scope.CLUSTER);
  }

  @Test
  public void testSessionRenewalResponse() throws IOException {
    testRunner.setProperty(QuerySOQL.OBJECT_NAME, "Account");
    testRunner.setProperty(QuerySOQL.FIELD_NAMES, "Id");
    AtomicReference<String> token = new AtomicReference<>("invalidToken");
    when(mockAuthService.getToken()).thenAnswer(invocationOnMock -> token.get());
    doAnswer(invocationOnMock -> {
      token.set("validToken");
      return null;
    }).when(mockAuthService).renew();
    testRunner.setIncomingConnection(false);

    testRunner.run();
    testRunner.assertTransferCount(QuerySOQL.REL_SUCCESS, 1);
    assertResult("fixtures/flowfile/query_no_limit.json");
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
        "/services/data/v46.0/query?q=SELECT%20Id%20FROM%20Account%20ORDER%20BY%20Id%20ASC",
        createMockResponse("fixtures/soql_id_no_initial_value.json", 200));
    mockResponseMap.put(
        "/services/data/v46.0/query?q=SELECT%20Id%20FROM%20Account%20WHERE%20Id%20%3E%20%270012p00002OV0hBAAT%27%20ORDER%20BY%20Id%20ASC",
        createMockResponse("fixtures/soql_multi_id_2.json", 200));
    mockResponseMap.put(
        "/services/data/v46.0/query?q=SELECT%20Id%20FROM%20Account%20WHERE%20Id%20%3E%20%270012p00002OV0hKAAT%27%20ORDER%20BY%20Id%20ASC",
        createMockResponse("fixtures/soql_empty.json", 200));
    mockResponseMap.put(
        "/services/data/v46.0/query?q=SELECT%20Id%20FROM%20Account%20WHERE%20Id%20%3E%20%270000000000000000%27%20ORDER%20BY%20Id%20ASC",
        createMockResponse("fixtures/soql_multi_id_1.json", 200));
    mockResponseMap.put(
        "/services/data/v46.0/query/nextQueryId",
        createMockResponse("fixtures/soql_multi_2.json", 200));
    mockResponseMap.put(
        "/services/data/v46.0/query/nextQueryId_id",
        createMockResponse("fixtures/soql_multi_id_2.json", 200));
    mockResponseMap.put(
        "/services/data/v46.0/sobjects/Account/describe",
        createMockResponse("fixtures/meta_describe.json", 200));

    return getDispatcher(mockResponseMap);

  }
}