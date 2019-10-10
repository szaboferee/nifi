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

import java.io.StringReader;
import java.time.Clock;

import javax.json.Json;
import javax.json.JsonObject;
import javax.json.JsonReader;
import javax.json.JsonString;

import org.apache.nifi.annotation.configuration.DefaultSchedule;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.scheduling.SchedulingStrategy;

@DefaultSchedule(strategy = SchedulingStrategy.TIMER_DRIVEN, period = "1 day")
@Tags({"salesforce", "sobject"})
public class ListUpdatedSObjects extends AbstractListSObjectsProcessor {

  public ListUpdatedSObjects() {
  super();
  }

  public ListUpdatedSObjects(Clock clock) {
    super(clock);
  }

  @Override
  protected String getListType() {
    return "/updated";
  }

  @Override
  protected String processResult(ProcessSession session, String sObjectName, String objectUrlPath, String result) {
    try (JsonReader reader = Json.createReader(new StringReader(result))) {
      JsonObject jsonObject = reader.readObject();
      jsonObject.getJsonArray("ids").forEach(jsonValue -> {
        FlowFile flowFile = session.create();
        session.putAttribute(flowFile, "salesforce.attributes.type", sObjectName);
        session.putAttribute(flowFile, "salesforce.attributes.url", objectUrlPath + ((JsonString) jsonValue).getString());
        session.transfer(flowFile, REL_SUCCESS);
      });
      return jsonObject.getString("latestDateCovered");
    }
  }

}
