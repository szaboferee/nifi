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

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.util.HashMap;

import org.apache.nifi.components.state.Scope;
import org.apache.nifi.util.MockFlowFile;
import org.junit.Before;
import org.junit.Test;

import okhttp3.mockwebserver.Dispatcher;
import okhttp3.mockwebserver.MockResponse;

public class ListUpdatedSObjectsTest extends SalesForceProcessorTestBase {

  @Override
  @Before
  public void setUp() throws Exception {
    super.setUp();
    setupTestRunner(new ListUpdatedSObjects(Clock.fixed(Instant.parse("2019-06-15T00:00:00.00Z"), ZoneId.of("UTC"))));

  }

  @Test
  public void testListUpdated() {

    testRunner.setProperty(ListUpdatedSObjects.SOBJECT_NAME, "Account");
    testRunner.setProperty(ListUpdatedSObjects.START_DATE, "2019-06-15T00:00:00+00:00");
    testRunner.setProperty(ListUpdatedSObjects.END_DATE, "2019-10-15T00:00:00+00:00");

    testRunner.run(1);
    testRunner.assertTransferCount(ListUpdatedSObjects.REL_SUCCESS, 3);

    MockFlowFile flowFile = testRunner.getFlowFilesForRelationship(ListUpdatedSObjects.REL_SUCCESS).get(0);

    flowFile.assertAttributeEquals("salesforce.attributes.type", "Account");
    flowFile.assertAttributeEquals("salesforce.attributes.url", "/services/data/v46.0/sobjects/Account/0012p00002NlZRyAAN");
    testRunner.getStateManager().assertStateEquals("lastDate", "2019-07-11T06:40:00.000+0000",  Scope.CLUSTER );

  }

  @Test
  public void testListUpdatedWithCurrentTime() {

    testRunner.setProperty(ListUpdatedSObjects.SOBJECT_NAME, "Account");

    testRunner.run(1);
    testRunner.assertTransferCount(ListUpdatedSObjects.REL_SUCCESS, 3);

    MockFlowFile flowFile = testRunner.getFlowFilesForRelationship(ListUpdatedSObjects.REL_SUCCESS).get(0);

    flowFile.assertAttributeEquals("salesforce.attributes.type", "Account");
    flowFile.assertAttributeEquals("salesforce.attributes.url", "/services/data/v46.0/sobjects/Account/0012p00002NlZRyAAN");
    testRunner.getStateManager().assertStateEquals("lastDate", "2019-07-11T06:40:00.000+0000",  Scope.CLUSTER );
  }


  @Test
  public void testListUpdatedPolling() {

    testRunner.setProperty(ListUpdatedSObjects.SOBJECT_NAME, "Account");
    testRunner.setProperty(ListUpdatedSObjects.START_DATE, "2019-06-14T00:00:00+00:00");
    testRunner.setProperty(ListUpdatedSObjects.POLL, "true");
    testRunner.run(2);
    testRunner.assertTransferCount(ListUpdatedSObjects.REL_SUCCESS, 3);

    MockFlowFile flowFile = testRunner.getFlowFilesForRelationship(ListUpdatedSObjects.REL_SUCCESS).get(0);

    flowFile.assertAttributeEquals("salesforce.attributes.type", "Account");
    flowFile.assertAttributeEquals("salesforce.attributes.url", "/services/data/v46.0/sobjects/Account/0012p00002NlZRyAAN");
    testRunner.getStateManager().assertStateEquals("lastDate", "2019-07-11T06:40:00.000+0000",  Scope.CLUSTER );
  }


  @Override
  protected Dispatcher getDispatcher() {
    HashMap<String, MockResponse> mockResponseMap = new HashMap<>();
    mockResponseMap.put(
        "/services/data/v46.0/sobjects/Account/updated?start=2019-06-14T00%3A00%3A00%2B00%3A00&end=2019-07-14T00%3A00%3A00%2B00%3A00",
        createMockResponse("fixtures/list_updated_poll_1.json", 200));
    mockResponseMap.put(
        "/services/data/v46.0/sobjects/Account/updated?start=2019-06-20T00%3A00%3A00%2B00%3A00&end=2019-07-20T00%3A00%3A00%2B00%3A00",
        createMockResponse("fixtures/list_updated_poll_2.json", 200));
    mockResponseMap.put(
        "/services/data/v46.0/sobjects/Account/updated?start=2019-06-15T00%3A00%3A00%2B00%3A00&end=2019-07-15T00%3A00%3A00%2B00%3A00",
        createMockResponse("fixtures/list_updated.json", 200));
    mockResponseMap.put(
        "/services/data/v46.0/sobjects/Account/updated?start=2019-06-15T00%3A00%3A00%2B00%3A00&end=2019-10-15T00%3A00%3A00%2B00%3A00",
        createMockResponse("fixtures/list_updated.json", 200));

    return getDispatcher(mockResponseMap);
  }
}