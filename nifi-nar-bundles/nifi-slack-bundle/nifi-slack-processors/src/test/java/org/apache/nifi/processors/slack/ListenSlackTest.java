package org.apache.nifi.processors.slack;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.function.Consumer;

import org.apache.nifi.processors.slack.controllers.SlackConnectionService;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Test;
import org.mockito.internal.stubbing.answers.CallsRealMethods;

public class ListenSlackTest {

  public static final String SERVICE_ID = "testId";

  @Test
  public void test1() throws InitializationException {
    ListenSlack listenSlack = mock(ListenSlack.class, new CallsRealMethods());
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

    verify(listenSlack, times(1)).onTrigger(any(), any());

    messageHandler[0].accept("{\"type\":\"hello\"}");

    runner.run();
    runner.assertAllFlowFilesTransferred(ListenSlack.MATCHED_MESSAGES_RELATIONSHIP, 1);


  }
}