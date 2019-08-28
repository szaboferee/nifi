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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.nifi.annotation.behavior.Stateful;
import org.apache.nifi.annotation.behavior.TriggerSerially;
import org.apache.nifi.annotation.configuration.DefaultSchedule;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.Validator;
import org.apache.nifi.components.state.Scope;
import org.apache.nifi.components.state.StateManager;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.scheduling.SchedulingStrategy;

@Stateful(scopes = Scope.CLUSTER, description = "TODO")
@DefaultSchedule(strategy = SchedulingStrategy.TIMER_DRIVEN, period = "1 hour")
@TriggerSerially
public abstract class AbstractListSObjectsProcessor extends AbstractSalesForceProcessor {

  static final PropertyDescriptor SOBJECT_NAME = new PropertyDescriptor.Builder()
      .name("sobject-name")
      .displayName("SObject Name")
      .description("")
      .required(true)
      .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
      .build();

  static final PropertyDescriptor START_DATE = new PropertyDescriptor.Builder()
      .name("start-date")
      .displayName("Start Date")
      .addValidator(Validator.VALID)
      .required(false)
      .build();

  static final PropertyDescriptor END_DATE = new PropertyDescriptor.Builder()
      .name("end-date")
      .displayName("End Date")
      .required(false)
      .addValidator(Validator.VALID)
      .build();

  static final PropertyDescriptor POLL = new PropertyDescriptor.Builder()
      .name("poll")
      .displayName("Poll")
      .required(true)
      .allowableValues("true", "false")
      .defaultValue("false")
      .build();

  static final PropertyDescriptor POLL_INTERVAL = new PropertyDescriptor.Builder()
      .name("poll-interval")
      .displayName("Poll Interval")
      .required(false)
      .build();


  protected abstract String processResult(ProcessSession session, String sObjectName, String objectUrlPath, String result);

  protected abstract String getListType();

  @Override
  protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
    List<PropertyDescriptor> propertyDescriptors = new ArrayList<>(super.getSupportedPropertyDescriptors());
    propertyDescriptors.addAll(Arrays.asList(SOBJECT_NAME, START_DATE, END_DATE, POLL));
    return propertyDescriptors;
  }

  @Override
  public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
    boolean poll = context.getProperty(POLL).asBoolean();
    if (poll) {
      try {
        restoreState(context);
      } catch (IOException e) {
        getLogger().error("Failed to restore processor state; yielding", e);
        context.yield();
        return;
      }
    } else {
    }

    String startDate = context.getProperty(START_DATE).getValue();
    String endDate = context.getProperty(END_DATE).getValue();
    String sObjectName = context.getProperty(SOBJECT_NAME).getValue();
    String version = context.getProperty(API_VERSION).getValue();


    String path = getVersionedPath(version, "/sobjects/" + sObjectName + getListType());
    String objectUrlPath = getVersionedPath(version, "/sobjects/" + sObjectName + "/");
    Map<String, String> queryParams = new HashMap<>();

    queryParams.put("start", startDate);
    queryParams.put("end", endDate);
    String result = doGetRequest(path, queryParams);

    String lastDateCovered = processResult(session, sObjectName, objectUrlPath, result);
    persistState(context, lastDateCovered);
  }

  private void persistState(ProcessContext context, String lastDateCovered) {
    try {
      Map<String, String> state = new HashMap<>();
      state.put("lastDate", lastDateCovered);
      context.getStateManager().setState(state, Scope.CLUSTER);
    } catch (IOException e) {
      getLogger().error("Failed to save cluster-wide state. If NiFi is restarted, data duplication may occur", e);
    }
  }

  private String restoreState(ProcessContext context) throws IOException {
    StateManager stateManager = context.getStateManager();
    String lastDate = stateManager.getState(Scope.CLUSTER).get("lastDate");
    if (lastDate == null) {
      String startDate = context.getProperty(START_DATE).getValue();
      if (startDate == null) {
        lastDate = "aaa";
      } else {
        lastDate = startDate;
      }
    }

    return lastDate;
  }
}
