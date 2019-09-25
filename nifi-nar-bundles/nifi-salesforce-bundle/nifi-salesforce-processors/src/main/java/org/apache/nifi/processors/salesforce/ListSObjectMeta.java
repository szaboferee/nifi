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

import javax.json.Json;
import javax.json.JsonArray;
import javax.json.JsonObject;
import javax.json.JsonReader;
import javax.json.JsonValue;

import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.configuration.DefaultSchedule;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.scheduling.SchedulingStrategy;

@InputRequirement(Requirement.INPUT_ALLOWED)
@DefaultSchedule(strategy = SchedulingStrategy.TIMER_DRIVEN, period = "1 day")
@Tags({"salesforce", "sobject"})
@WritesAttributes(
  @WritesAttribute(attribute = "salesforce.attributes.url", description = "TBD")
)
public class ListSObjectMeta extends AbstractSalesForceProcessor {

  private String path;

  @OnScheduled
  public void setup(ProcessContext context) {
    String version = context.getProperty(API_VERSION).evaluateAttributeExpressions().getValue();
    path = getVersionedPath(version, "/sobjects");
  }

  @Override
  public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
    String response = doGetRequest(path);

    try (JsonReader reader = Json.createReader(new StringReader(response))) {
      JsonArray sobjects = reader.readObject().getJsonArray("sobjects");
      sobjects.forEach(jsonValue -> parseJsonToFlowFile(session, jsonValue));
    }
  }

  private void parseJsonToFlowFile(ProcessSession session, JsonValue jsonValue) {
    if (jsonValue instanceof JsonObject) {
      FlowFile flowFile = session.create();
      JsonObject jsonObject = (JsonObject) jsonValue;
      session.write(flowFile, out -> out.write(jsonObject.toString().getBytes()));
      JsonObject urls = jsonObject.getJsonObject("urls");
      session.putAttribute(flowFile, "salesforce.attributes.url", urls.getString("sobject"));
      session.putAttribute(flowFile, "salesforce.attributes.objectName", jsonObject.getString("name"));
      session.transfer(flowFile, REL_SUCCESS);
    }
  }
}
