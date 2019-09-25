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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import javax.json.Json;
import javax.json.JsonObject;
import javax.json.JsonReader;

import org.apache.nifi.annotation.behavior.EventDriven;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.ReadsAttribute;
import org.apache.nifi.annotation.behavior.ReadsAttributes;
import org.apache.nifi.annotation.configuration.DefaultSchedule;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.Validator;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.scheduling.SchedulingStrategy;


@InputRequirement(Requirement.INPUT_ALLOWED)
@ReadsAttributes({
    @ReadsAttribute(attribute = "salesforce.attributes.url", description = "TODO"),
    @ReadsAttribute(attribute = "salesforce.attributes.objectName", description = "TODO")
})
@Tags({"salesforce", "sobject"})
public class FetchSObjectMeta extends AbstractSalesForceProcessor {

  static final PropertyDescriptor USE_DESCRIBE = new PropertyDescriptor.Builder()
      .name("use-describe")
      .displayName("Use Describe")
      .defaultValue("false")
      .allowableValues("true", "false")
      .addValidator(Validator.VALID)
      .required(true)
      .build();

  static final PropertyDescriptor SOBJECT_NAME = new PropertyDescriptor.Builder()
      .name("sobject-name")
      .displayName("SObject Name")
      .required(false)
      .addValidator(Validator.VALID)
      .build();
  private String objectName;
  private String version;
  private Boolean useDescribe;
  private String path;


  @Override
  protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
    List<PropertyDescriptor> propertyDescriptors = new ArrayList<>(super.getSupportedPropertyDescriptors());
    propertyDescriptors.addAll(Arrays.asList(SOBJECT_NAME, USE_DESCRIBE));
    return propertyDescriptors;
  }

  @OnScheduled
  public void setup(ProcessContext context) {
    objectName = context.getProperty(SOBJECT_NAME).getValue();
    version = context.getProperty(API_VERSION).evaluateAttributeExpressions().getValue();
    useDescribe = context.getProperty(USE_DESCRIBE).asBoolean();
    path = getVersionedPath(version, "/sobjects/" + objectName);
  }

  @Override
  public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {

    String url;
    FlowFile flowFile;
    if (context.hasIncomingConnection()) {
      flowFile = session.get();
      if (flowFile == null) {
        context.yield();
        return;
      }
      url = flowFile.getAttribute("salesforce.attributes.url");

    } else {
      flowFile = session.create();
      url = path;
      session.putAttribute(flowFile, "salesforce.attributes.url", path);
      session.putAttribute(flowFile, "salesforce.attributes.objectName", objectName);
    }

    if (useDescribe) {
      url += "/describe";
    }


    String response = doGetRequest(url);

    if (useDescribe) {
      List<String> fields = getFieldList(response);
      session.putAttribute(flowFile, "salesforce.attributes.fieldList", String.join(",", fields));
    }

    session.write(flowFile, out -> out.write(response.getBytes()));

    session.transfer(flowFile, REL_SUCCESS);
  }

  private List<String> getFieldList(String response) {
    try (JsonReader reader = Json.createReader(new StringReader(response))) {
      JsonObject jsonObject = reader.readObject();
      return jsonObject.getJsonArray("fields").stream()
          .map(jsonValue -> ((JsonObject) jsonValue).getString("name"))
          .collect(Collectors.toList());
    }
  }
}
