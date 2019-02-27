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
package org.apache.nifi.processors.slack;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.List;
import java.util.function.Consumer;

import org.apache.nifi.processors.slack.controllers.SlackConnectionService;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Test;

public class ListenSlackTest {

  public static final String SERVICE_ID = "testId";
  public static final List<String> INPUT_MESSAGES = Arrays.asList(
    "{\"type\":\"hello\"}",
    "{\"type\":\"message\"}",
    "{\"type\":\"desktop_notification\"}",
    "{\"type\":\"file_shared\"}",
    "",
    "{}"
  );

  @Test
  public void testEmptyMessageTypesMatchesAnything() throws InitializationException {
    ListenSlack listenSlack = new ListenSlack();
    TestRunner runner = TestRunners.newTestRunner(listenSlack);

    SlackConnectionService slackConnectionService = mock(SlackConnectionService.class);
    when(slackConnectionService.getIdentifier()).thenReturn(SERVICE_ID);
    when(slackConnectionService.isProcessorRegistered(listenSlack)).thenReturn(false, true);
    final Consumer[] messageHandler = new Consumer[1];
    doAnswer(invocation -> {
      messageHandler[0] = invocation.getArgumentAt(1, Consumer.class);
      return null;
    }).when(slackConnectionService).registerProcessor(eq(listenSlack), any());


    runner.setProperty(ListenSlack.SLACK_CONNECTION_SERVICE, SERVICE_ID);
    runner.addControllerService(SERVICE_ID, slackConnectionService);

    runner.enableControllerService(slackConnectionService);
    runner.run();

    INPUT_MESSAGES.forEach(messageHandler[0]);

    runner.assertAllFlowFilesTransferred(ListenSlack.MATCHED_MESSAGES_RELATIONSHIP, INPUT_MESSAGES.size());

    List<MockFlowFile> matched = runner.getFlowFilesForRelationship(ListenSlack.MATCHED_MESSAGES_RELATIONSHIP);
    for (int i = 0; i < INPUT_MESSAGES.size(); i++) {
      matched.get(i).assertContentEquals(INPUT_MESSAGES.get(i));
    }
  }


  @Test
  public void testMessageTypeMatchesProperMessages() throws InitializationException {
    ListenSlack listenSlack = new ListenSlack();
    TestRunner runner = TestRunners.newTestRunner(listenSlack);

    SlackConnectionService slackConnectionService = mock(SlackConnectionService.class);
    when(slackConnectionService.getIdentifier()).thenReturn(SERVICE_ID);
    when(slackConnectionService.isProcessorRegistered(listenSlack)).thenReturn(false, true);
    final Consumer[] messageHandler = new Consumer[1];
    doAnswer(invocation -> {
      messageHandler[0] = invocation.getArgumentAt(1, Consumer.class);
      return null;
    }).when(slackConnectionService).registerProcessor(eq(listenSlack), any());


    runner.setProperty(ListenSlack.SLACK_CONNECTION_SERVICE, SERVICE_ID);
    runner.setProperty(ListenSlack.MESSAGE_TYPES, "message");
    runner.addControllerService(SERVICE_ID, slackConnectionService);

    runner.enableControllerService(slackConnectionService);
    runner.run();

    INPUT_MESSAGES.forEach(messageHandler[0]);

    runner.assertTransferCount(ListenSlack.MATCHED_MESSAGES_RELATIONSHIP, 1);

    List<MockFlowFile> matched = runner.getFlowFilesForRelationship(ListenSlack.MATCHED_MESSAGES_RELATIONSHIP);
    matched.get(0).assertContentEquals(INPUT_MESSAGES.get(1));
  }


  @Test
  public void testMultipleMessageTypesMatchesProperMessages() throws InitializationException {
    ListenSlack listenSlack = new ListenSlack();
    TestRunner runner = TestRunners.newTestRunner(listenSlack);

    SlackConnectionService slackConnectionService = mock(SlackConnectionService.class);
    when(slackConnectionService.getIdentifier()).thenReturn(SERVICE_ID);
    when(slackConnectionService.isProcessorRegistered(listenSlack)).thenReturn(false, true);
    final Consumer[] messageHandler = new Consumer[1];
    doAnswer(invocation -> {
      messageHandler[0] = invocation.getArgumentAt(1, Consumer.class);
      return null;
    }).when(slackConnectionService).registerProcessor(eq(listenSlack), any());


    runner.setProperty(ListenSlack.SLACK_CONNECTION_SERVICE, SERVICE_ID);
    runner.setProperty(ListenSlack.MESSAGE_TYPES, "message,file_shared");
    runner.addControllerService(SERVICE_ID, slackConnectionService);

    runner.enableControllerService(slackConnectionService);
    runner.run();

    INPUT_MESSAGES.forEach(messageHandler[0]);

    runner.assertTransferCount(ListenSlack.MATCHED_MESSAGES_RELATIONSHIP, 2);

    List<MockFlowFile> matched = runner.getFlowFilesForRelationship(ListenSlack.MATCHED_MESSAGES_RELATIONSHIP);
    matched.get(0).assertContentEquals(INPUT_MESSAGES.get(1));
    matched.get(1).assertContentEquals(INPUT_MESSAGES.get(3));
  }

  @Test
  public void testRegisterAndDeregisterHappens() throws InitializationException, InterruptedException {
    ListenSlack listenSlack = new ListenSlack();
    TestRunner runner = TestRunners.newTestRunner(listenSlack);

    SlackConnectionService slackConnectionService = mock(SlackConnectionService.class);
    when(slackConnectionService.getIdentifier()).thenReturn(SERVICE_ID);
    when(slackConnectionService.isProcessorRegistered(listenSlack)).thenReturn( true, true);

    runner.setProperty(ListenSlack.SLACK_CONNECTION_SERVICE, SERVICE_ID);
    runner.addControllerService(SERVICE_ID, slackConnectionService);
    runner.enableControllerService(slackConnectionService);
    runner.run();

    verify(slackConnectionService, times(1)).isProcessorRegistered(eq(listenSlack));
    verify(slackConnectionService, times(1)).registerProcessor(eq(listenSlack), any());
    verify(slackConnectionService, times(1)).deregisterProcessor(eq(listenSlack));
  }
}