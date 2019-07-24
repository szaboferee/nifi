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

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Map;
import java.util.Objects;

import org.apache.nifi.processors.salesforce.controllers.SalesForceAuthService;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.After;
import org.junit.Before;

import okhttp3.mockwebserver.Dispatcher;
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import okhttp3.mockwebserver.RecordedRequest;

abstract public class SalesForceProcessorTestBase {

  private static final String MOCK_AUTH_SERVICE_ID = "mockAuthService";
  private static final String TEST_API_VERSION = "v46.0";
  SalesForceAuthService mockAuthService;
  private MockWebServer mockServer;
  TestRunner testRunner;

  protected abstract Dispatcher getDispatcher();

  @Before
  public void setUp() throws Exception {
    mockServer = new MockWebServer();
    mockServer.setDispatcher(getDispatcher());
    mockServer.start();
    String instanceUrl = "http://" + mockServer.getHostName() + ":" + mockServer.getPort();


    mockAuthService = mock(SalesForceAuthService.class);
    when(mockAuthService.getIdentifier()).thenReturn(MOCK_AUTH_SERVICE_ID);
    when(mockAuthService.getInstanceUrl()).thenReturn(instanceUrl);
    when(mockAuthService.getToken()).thenReturn("validToken");


  }

  void setupTestRunner(Class<? extends AbstractSalesForceProcessor> processorClass) throws InitializationException {
    testRunner = TestRunners.newTestRunner(processorClass);
    testRunner.addControllerService(MOCK_AUTH_SERVICE_ID, mockAuthService);
    testRunner.enableControllerService(mockAuthService);
    testRunner.setProperty(AbstractSalesForceProcessor.AUTH_SERVICE, MOCK_AUTH_SERVICE_ID);
    testRunner.setProperty(AbstractSalesForceProcessor.API_VERSION, TEST_API_VERSION);
  }

  @After
  public void tearDown() throws Exception {
    mockServer.shutdown();
  }

  MockResponse createMockResponse(String name, int responseCode) {
    MockResponse response = new MockResponse();
    response.setResponseCode(responseCode);
    String body = "";
    try {
      body = readFile(name);
    } catch (IOException e) {
      throw new RuntimeException("Error while reading file: " + name, e);
    }
    response.setBody(body);
    return response;
  }

  String readFile(String name) throws IOException {
    return new String(Files.readAllBytes(Paths.get(Objects.requireNonNull(getClass().getClassLoader().getResource(name)).getPath())));
  }

  Dispatcher getDispatcher(Map<String, MockResponse> mockResponseMap) {
    return new Dispatcher() {

      @Override
      public MockResponse dispatch(RecordedRequest recordedRequest) {
        String header = recordedRequest.getHeader("Authorization");
        if (header.equals("Bearer validToken")) {
          return mockResponseMap.getOrDefault(recordedRequest.getPath(), new MockResponse().setResponseCode(404));
        } else {
          return createMockResponse("fixtures/session_invalid.json", 401);
        }
      }
    };
  }
}
