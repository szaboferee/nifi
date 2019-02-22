package org.apache.nifi.processors.slack;

import static org.junit.Assert.*;

import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Test;

public class FetchSlackTest {

  @Test
  public void test1() {
    TestRunner testRunner = TestRunners.newTestRunner(FetchSlack.class);
  }

}