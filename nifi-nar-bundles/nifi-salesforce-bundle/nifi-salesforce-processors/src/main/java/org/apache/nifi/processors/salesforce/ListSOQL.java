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
import java.util.HashMap;
import java.util.List;

import javax.json.Json;
import javax.json.JsonArray;
import javax.json.JsonException;
import javax.json.JsonObject;
import javax.json.JsonReader;
import javax.json.JsonString;
import javax.json.JsonValue;
import javax.json.JsonValue.ValueType;

import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;

@InputRequirement(Requirement.INPUT_ALLOWED)
@Tags({"salesforce", "sobject"})
public class ListSOQL extends AbstractSalesForceProcessor {

  static final PropertyDescriptor QUERY = new PropertyDescriptor.Builder()
    .name("query")
    .displayName("Query")
    .description("")
    .required(true)
    .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
    .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
    .build();

  @Override
  protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
    List<PropertyDescriptor> propertyDescriptors = new ArrayList<>(super.getSupportedPropertyDescriptors());
    propertyDescriptors.addAll(Arrays.asList(QUERY));
    return propertyDescriptors;
  }

  @Override
  public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
    FlowFile requestFlowFile = session.get();
    if (context.hasIncomingConnection()) {
      if (requestFlowFile == null && context.hasNonLoopConnection()) {
        context.yield();
        return;
      }
    }

    String query = context.getProperty(QUERY).evaluateAttributeExpressions(requestFlowFile).getValue();
    String version = context.getProperty(API_VERSION).getValue();

    String path = getVersionedPath(version, "/query");
    HashMap<String, String> queryParams = new HashMap<>();
    queryParams.put("q", query);
    String result = doGetRequest(path, queryParams);

    parseResultsToFlowFiles(session, result, requestFlowFile);
    if (requestFlowFile != null) {
      session.remove(requestFlowFile);
    }
  }

  private void parseResultsToFlowFiles(ProcessSession session, String result, FlowFile requestFlowFile) {
    if (result.length() > 0) {
      try (JsonReader reader = Json.createReader(new StringReader(result))) {
        JsonObject jsonObject = reader.readObject();
        JsonArray records = jsonObject.getJsonArray("records");
        convertRecordsToFlowFiles(session, records, requestFlowFile);
        if (!jsonObject.getBoolean("done")) {
          String nextRecordsResult = doGetRequest(jsonObject.getString("nextRecordsUrl"));
          parseResultsToFlowFiles(session, nextRecordsResult, requestFlowFile);
        }
      } catch (JsonException e) {
        getLogger().error("Error while parsing result: [" + result + "]", e);
      }
    }
  }

  private void convertRecordsToFlowFiles(ProcessSession session, JsonArray records, FlowFile requestFlowFile) {
    for (int i = 0; i < records.size(); i++) {
      JsonObject jsonObject = records.getJsonObject(i);
      FlowFile flowFile = requestFlowFile == null ? session.create() : session.create(requestFlowFile);
      jsonObject.getJsonObject("attributes")
        .forEach((aKey, aValue) ->
          session.putAttribute(flowFile, "salesforce.attributes." + aKey, stringValue(aValue))
        );
      session.write(flowFile, out -> out.write(jsonObject.toString().getBytes()));
      session.transfer(flowFile, REL_SUCCESS);
    }
  }

  private String stringValue(JsonValue aValue) {
    if (aValue.getValueType() == ValueType.STRING) {
      return ((JsonString) aValue).getString();
    }
    return aValue.toString();
  }
}
