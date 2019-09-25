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
package org.apache.nifi.processors.salesforce.controllers;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Objects;

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.MockConfigurationContext;
import org.apache.nifi.util.MockControllerServiceInitializationContext;
import org.junit.Before;
import org.junit.Test;

import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;

public class OAuthSalesForceAuthServiceTest {

  private static final String TEST_INSTANCE_URL = "http://example.com";
  private static final String TEST_INSTANCE_URL2 = "http://example2.com";
  private static final String TEST_ACCESS_TOKEN = "myToken";
  private static final String TEST_ACCESS_TOKEN2 = "myToken2";
  private MockWebServer mockWebServer;
  private MockConfigurationContext context;
  private OAuthSalesForceAuthService service;


  @Before
  public void setUp() throws Exception {
    mockWebServer = new MockWebServer();
    mockWebServer.start();
    String loginUrl = "http://" + mockWebServer.getHostName() + ":" + mockWebServer.getPort();

    HashMap<PropertyDescriptor, String> properties = new HashMap<>();
    properties.put(OAuthSalesForceAuthService.LOGIN_URL, loginUrl);
    properties.put(OAuthSalesForceAuthService.CLIENT_ID,"test_id");
    properties.put(OAuthSalesForceAuthService.CLIENT_SECRET,"test_secret");
    properties.put(OAuthSalesForceAuthService.USERNAME,"test_username");
    properties.put(OAuthSalesForceAuthService.PASSWORD,"test_password");
    context = new MockConfigurationContext(properties, null);
    service = new OAuthSalesForceAuthService();
    MockControllerServiceInitializationContext initializationContext = new MockControllerServiceInitializationContext(service, "mock-service");
    service.initialize(initializationContext);

  }

  @Test
  public void testSuccessfulAuthHappensOnlyOnce() throws IOException, InitializationException {
    enqueueResponse("success.json", 200);

    service.onEnabled(context);

    String instanceUrl = service.getInstanceUrl();
    String instanceUrl2 = service.getInstanceUrl();
    String token = service.getToken();
    String token2 = service.getToken();
    int requestCount = mockWebServer.getRequestCount();

    assertEquals(TEST_INSTANCE_URL, instanceUrl);
    assertEquals(TEST_INSTANCE_URL, instanceUrl2);
    assertEquals(TEST_ACCESS_TOKEN, token);
    assertEquals(TEST_ACCESS_TOKEN, token2);
    assertEquals(1, requestCount);
  }

  @Test(expected = RuntimeException.class)
  public void testInvalid() throws IOException, InitializationException {
    enqueueResponse("invalid.json", 400);

    service.onEnabled(context);
  }


  @Test
  public void testRenew() throws IOException, InitializationException {
    enqueueResponse("success.json", 200);
    enqueueResponse("success2.json", 200);

    service.onEnabled(context);

    String instanceUrl = service.getInstanceUrl();
    String token = service.getToken();
    service.renew();
    String instanceUrl2 = service.getInstanceUrl();
    String token2 = service.getToken();
    int requestCount = mockWebServer.getRequestCount();

    assertEquals(TEST_INSTANCE_URL, instanceUrl);
    assertEquals(TEST_INSTANCE_URL2, instanceUrl2);
    assertEquals(TEST_ACCESS_TOKEN, token);
    assertEquals(TEST_ACCESS_TOKEN2, token2);
    assertEquals(2, requestCount);
  }


  private void enqueueResponse(String name, int responseCode) throws IOException {
    MockResponse response = new MockResponse();
    response.setResponseCode(responseCode);
    byte[] body = Files.readAllBytes(Paths.get(Objects.requireNonNull(getClass().getClassLoader().getResource(name)).getPath()));
    response.setBody(new String(body));
    mockWebServer.enqueue(response);
  }

}