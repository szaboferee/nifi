package org.apache.nifi.processors.slack;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.apache.nifi.processors.slack.controllers.SlackConnectionService;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Test;

public class ListenSlackTest {

  public static final String SERVICE_ID = "testId";

  @Test
  public void test1() throws InitializationException {
    TestRunner testRunner = TestRunners.newTestRunner(ListenSlack.class);
    SlackConnectionService slackConnectionService = mock(SlackConnectionService.class);
    when(slackConnectionService.getIdentifier()).thenReturn(SERVICE_ID);

    testRunner.addControllerService(SERVICE_ID, slackConnectionService);
  }
}