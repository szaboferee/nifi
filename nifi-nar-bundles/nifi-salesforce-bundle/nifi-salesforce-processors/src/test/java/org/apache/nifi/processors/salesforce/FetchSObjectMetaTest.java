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

public class FetchSObjectMetaTest extends SalesForceProcessorTestBase {

  @Override
  @Before
  public void setUp() throws Exception {
    super.setUp();
    setupTestRunner(FetchSObjectMeta.class);
  }

  @Test
  public void testFetchObjectMetaFromInput() throws IOException {

    HashMap<String, String> attributes = new HashMap<>();
    attributes.put("salesforce.attributes.url", "/services/data/v46.0/sobjects/Account");
    attributes.put("salesforce.attributes.objectName",  "Account");
    testRunner.enqueue("", attributes);

    testRunner.run();

    testRunner.assertTransferCount(FetchSObject.REL_SUCCESS, 1);
    MockFlowFile flowFile = testRunner.getFlowFilesForRelationship(FetchSObject.REL_SUCCESS).get(0);
    flowFile.assertContentEquals(readFile("fixtures/meta.json"));
    flowFile.assertAttributeEquals("salesforce.attributes.url", "/services/data/v46.0/sobjects/Account");
    flowFile.assertAttributeEquals("salesforce.attributes.objectName",  "Account");

  }

  @Test
  public void testFetchObjectMetaFromInputDescribe() throws IOException {

    HashMap<String, String> attributes = new HashMap<>();
    attributes.put("salesforce.attributes.url", "/services/data/v46.0/sobjects/Account");
    attributes.put("salesforce.attributes.objectName",  "Account");
    testRunner.enqueue("", attributes);

    testRunner.setProperty(FetchSObjectMeta.USE_DESCRIBE, "true");
    testRunner.run();

    testRunner.assertTransferCount(FetchSObject.REL_SUCCESS, 1);
    MockFlowFile flowFile = testRunner.getFlowFilesForRelationship(FetchSObject.REL_SUCCESS).get(0);
    flowFile.assertContentEquals(readFile("fixtures/meta_describe.json"));
    flowFile.assertAttributeEquals("salesforce.attributes.url", "/services/data/v46.0/sobjects/Account");
    flowFile.assertAttributeEquals("salesforce.attributes.objectName",  "Account");

  }

  @Test
  public void testFetchObjectMetaWithoutInput() throws IOException {

    testRunner.setIncomingConnection(false);
    testRunner.setProperty(FetchSObjectMeta.SOBJECT_NAME, "Account");
    testRunner.run();

    testRunner.assertTransferCount(FetchSObject.REL_SUCCESS, 1);
    MockFlowFile flowFile = testRunner.getFlowFilesForRelationship(FetchSObject.REL_SUCCESS).get(0);
    flowFile.assertContentEquals(readFile("fixtures/meta.json"));
    flowFile.assertAttributeEquals("salesforce.attributes.url", "/services/data/v46.0/sobjects/Account");
    flowFile.assertAttributeEquals("salesforce.attributes.objectName",  "Account");

  }

  @Test
  public void testFetchObjectMetaWithoutInputDescribe() throws IOException {

    testRunner.setIncomingConnection(false);
    testRunner.setProperty(FetchSObjectMeta.SOBJECT_NAME, "Account");
    testRunner.setProperty(FetchSObjectMeta.USE_DESCRIBE, "true");
    testRunner.run();

    testRunner.assertTransferCount(FetchSObject.REL_SUCCESS, 1);
    MockFlowFile flowFile = testRunner.getFlowFilesForRelationship(FetchSObject.REL_SUCCESS).get(0);
    flowFile.assertContentEquals(readFile("fixtures/meta_describe.json"));
    flowFile.assertAttributeEquals("salesforce.attributes.url", "/services/data/v46.0/sobjects/Account");
    flowFile.assertAttributeEquals("salesforce.attributes.objectName",  "Account");
    flowFile.assertAttributeEquals("salesforce.attributes.fieldList",
      "Id,IsDeleted,MasterRecordId,Name,Type,ParentId,BillingStreet,BillingCity,BillingState,BillingPostalCode,BillingCountry,BillingLatitude," +
        "BillingLongitude,BillingGeocodeAccuracy,BillingAddress,ShippingStreet,ShippingCity,ShippingState,ShippingPostalCode,ShippingCountry,ShippingLatitude," +
        "ShippingLongitude,ShippingGeocodeAccuracy,ShippingAddress,Phone,Fax,AccountNumber,Website,PhotoUrl,Sic,Industry,AnnualRevenue,NumberOfEmployees,Ownership," +
        "TickerSymbol,Description,Rating,Site,OwnerId,CreatedDate,CreatedById,LastModifiedDate,LastModifiedById,SystemModstamp,LastActivityDate,LastViewedDate," +
        "LastReferencedDate,Jigsaw,JigsawCompanyId,CleanStatus,AccountSource,DunsNumber,Tradestyle,NaicsCode,NaicsDesc,YearStarted,SicDesc,DandbCompanyId,CustomerPriority__c," +
        "SLA__c,Active__c,NumberofLocations__c,UpsellOpportunity__c,SLASerialNumber__c,SLAExpirationDate__c");

  }


  @Override
  protected Dispatcher getDispatcher() {
    HashMap<String, MockResponse> mockResponseMap = new HashMap<>();
    mockResponseMap.put("/services/data/v46.0/sobjects/Account", createMockResponse("fixtures/meta.json", 200));
    mockResponseMap.put("/services/data/v46.0/sobjects/Account/describe", createMockResponse("fixtures/meta_describe.json", 200));

    return getDispatcher(mockResponseMap);
  }
}