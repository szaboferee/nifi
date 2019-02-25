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


import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.json.Json;
import javax.json.JsonArray;
import javax.json.JsonException;
import javax.json.JsonObject;
import javax.json.JsonValue;
import javax.json.JsonValue.ValueType;

import org.apache.http.Header;
import org.apache.http.HttpHeaders;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.client.methods.RequestBuilder;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.message.BasicHeader;
import org.apache.nifi.annotation.behavior.EventDriven;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.lifecycle.OnShutdown;
import org.apache.nifi.annotation.lifecycle.OnUnscheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;

@Tags({"slack", "download", "fetch", "attachment"})
@CapabilityDescription("Downloading attachments from slack messages")
@EventDriven
@InputRequirement(Requirement.INPUT_REQUIRED)
public class FetchSlack extends AbstractProcessor {

  static final PropertyDescriptor API_TOKEN = new PropertyDescriptor.Builder()
    .name("API Token")
    .description("Token for the slack bot user")
    .required(true)
    .sensitive(true)
    .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
    .build();

  static final Relationship REL_SUCCESS_REQ = new Relationship.Builder()
    .name("Original")
    .description("The original FlowFile will be routed upon success (2xx status codes). It will have new attributes detailing the "
      + "success of the request.")
    .build();

  static final Relationship REL_RESPONSE = new Relationship.Builder()
    .name("Response")
    .description("A Response FlowFile will be routed upon success (2xx status codes). If the 'Output Response Regardless' property "
      + "is true then the response will be sent to this relationship regardless of the status code received.")
    .build();

  static final Relationship REL_RETRY = new Relationship.Builder()
    .name("Retry")
    .description("The original FlowFile will be routed on any status code that can be retried (5xx status codes). It will have new "
      + "attributes detailing the request.")
    .build();

  static final Relationship REL_NO_RETRY = new Relationship.Builder()
    .name("No Retry")
    .description("The original FlowFile will be routed on any status code that should NOT be retried (1xx, 3xx, 4xx status codes).  "
      + "It will have new attributes detailing the request.")
    .build();

  static final Relationship REL_FAILURE = new Relationship.Builder()
    .name("Failure")
    .description("The original FlowFile will be routed on any type of connection failure, timeout or general exception. "
      + "It will have new attributes detailing the request.")
    .build();

  private static final Set<Relationship> RELATIONSHIPS = Collections.unmodifiableSet(new HashSet<>(Arrays.asList(
    REL_SUCCESS_REQ, REL_RESPONSE, REL_RETRY, REL_NO_RETRY, REL_FAILURE)));

  private static final List<PropertyDescriptor> DESCRIPTORS = Collections.unmodifiableList(Arrays.asList(
    API_TOKEN));

  private CloseableHttpClient client;
  private Pattern filenamePattern = Pattern.compile(".*filename=\"([^\"]+)\".*");

  @Override
  public Set<Relationship> getRelationships() {
    return RELATIONSHIPS;
  }

  @Override
  public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
    return DESCRIPTORS;
  }

  @OnScheduled
  public void createClient(final ProcessContext context) {
    Header header = new BasicHeader(
      HttpHeaders.AUTHORIZATION, "Bearer " + context.getProperty(API_TOKEN).getValue());
    List<Header> headers = Collections.singletonList(header);
    client = HttpClients.custom().setDefaultHeaders(headers).build();
  }

  @OnUnscheduled
  @OnShutdown
  public void releaseClient() {
    try {
      client.close();
    } catch (IOException e) {
      getLogger().warn("Error while closing HttpClient", e);
    }
    client = null;
  }

  @Override
  public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {

    FlowFile requestFlowFile = session.get();
    if (requestFlowFile == null) {
      context.yield();

      return;
    }

    try (InputStream requestInputStream = session.read(requestFlowFile)) {
      List<JsonObject> filesToDownload = collectFilesToDownload(requestInputStream);

      if (filesToDownload.isEmpty()) {
        session.transfer(requestFlowFile, REL_FAILURE);
      } else {
        downloadFiles(session, requestFlowFile, filesToDownload);
      }
    } catch (JsonException | IOException e) {
      session.transfer(requestFlowFile, REL_FAILURE);
    }
  }

  private void downloadFiles(ProcessSession session, FlowFile requestFlowFile, List<JsonObject> filesToDownload) {
    try {
      ArrayList<FlowFile> successfulResponses = new ArrayList<>();
      for (JsonObject file : filesToDownload) {
        HttpUriRequest request = RequestBuilder.get().setUri(file.getString("url_private_download")).build();
        HttpResponse response = client.execute(request);
        int statusCode = response.getStatusLine().getStatusCode();
        if (inStatusCodeFamily(200, statusCode)) {
          FlowFile responseFlowFile = session.create();
          session.write(responseFlowFile, out -> response.getEntity().writeTo(out));
          Map<String, String> attributes = requestFlowFile.getAttributes();
          attributes = fillAttributes(attributes, file, response);
          responseFlowFile = session.putAllAttributes(responseFlowFile, attributes);
          successfulResponses.add(responseFlowFile);
        } else if (inStatusCodeFamily(100, statusCode)
          || inStatusCodeFamily(300, statusCode)
          || inStatusCodeFamily(400, statusCode)) {
          session.transfer(requestFlowFile, REL_NO_RETRY);
          break;
        } else if (inStatusCodeFamily(500, statusCode)) {
          session.transfer(requestFlowFile, REL_RETRY);
          break;
        }
      }

      if (successfulResponses.size() == filesToDownload.size()) {
        successfulResponses.forEach(flowFile -> session.transfer(flowFile, REL_RESPONSE));
        session.transfer(requestFlowFile, REL_SUCCESS_REQ);
      }
    } catch (IOException e) {
      session.transfer(requestFlowFile, REL_FAILURE);
    }
  }

  private List<JsonObject> collectFilesToDownload(InputStream requestInputStream) {
    List<JsonObject> filesToDownload = new ArrayList<>();
    JsonObject jsonObject = Json.createReader(requestInputStream).readObject();
    JsonArray files = jsonObject.getJsonArray("files");
    if (files != null) {
      for (JsonValue file : files) {
        String url = ((JsonObject) file).getString("url_private_download");
        if (url != null) {
          filesToDownload.add((JsonObject)file);
        }
      }
    }
    return filesToDownload;
  }

  private HashMap<String, String> fillAttributes(Map<String, String> attributes, JsonObject file, HttpResponse response) {
    HashMap<String, String> map = new HashMap<>(attributes);
    String value = response.getFirstHeader("Content-Disposition").getValue();
    Matcher matcher = filenamePattern.matcher(value);
    if (matcher.matches()){
      map.put("response.filename", matcher.group(1));
    }

    fromheader(map, response, "Content-Length", "response.content-length");
    fromheader(map, response, "Content-type", "response.content-type");
    fromheader(map, response, "Date", "response.date");

    fromJson(map, file, "name", "message.name");
    fromJson(map, file, "mimetype", "message.mimetype");
    fromJson(map, file, "filetype", "message.filetype");
    fromJson(map, file, "title", "message.title");
    fromJson(map, file, "id", "message.id");
    fromJson(map, file, "created", "message.created");
    fromJson(map, file, "size", "message.size");

    return map;
  }

  private void fromJson(Map<String, String> attributes, JsonObject file, String jsonKey, String attribute) {
    ValueType valueType = file.get(jsonKey).getValueType();
    String stringValue = null;
    if (valueType.equals(ValueType.NUMBER)) {
      stringValue = String.valueOf(file.getInt(jsonKey));
    } else if (valueType.equals(ValueType.STRING)) {
      stringValue = file.getString(jsonKey);
    }

    if (stringValue != null) {
      attributes.put(attribute, stringValue);
    }
  }

  private void fromheader(Map<String, String> attributes, HttpResponse response, String header, String attribute) {
    Header firstHeader = response.getFirstHeader(header);
    if (firstHeader != null)
    attributes.put(attribute, firstHeader.getValue());
  }

  private boolean inStatusCodeFamily(int family, int statusCode) {
    return statusCode >= family && statusCode < family + 100;
  }

}
