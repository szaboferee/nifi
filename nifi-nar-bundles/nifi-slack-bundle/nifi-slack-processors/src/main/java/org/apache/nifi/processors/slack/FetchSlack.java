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
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import javax.json.Json;
import javax.json.JsonArray;
import javax.json.JsonObject;
import javax.json.JsonValue;

import org.apache.http.Header;
import org.apache.http.HttpHeaders;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.client.methods.RequestBuilder;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.message.BasicHeader;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.lifecycle.OnShutdown;
import org.apache.nifi.annotation.lifecycle.OnUnscheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.Validator;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;

@Tags({"slack", "download", "fetch", "attachment"})
@CapabilityDescription("Downloading attachments from slack messages")
public class FetchSlack extends AbstractProcessor {

  private static final PropertyDescriptor API_TOKEN = new PropertyDescriptor.Builder()
    .name("API Token")
    .description("Token for the slack bot user")
    .required(true)
    .sensitive(true)
    .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
    .build();

  private static final PropertyDescriptor MESSAGE_TYPES = new PropertyDescriptor
    .Builder()
    .name("Message types")
    .description("Message types to listen to. It will filter messages with the given types or " +
      "listen to every type if empty. It is a coma separated list like: message,file_shared")
    .required(false)
    .addValidator(Validator.VALID)
    .build();

  public static final Relationship REL_SUCCESS_REQ = new Relationship.Builder()
    .name("Original")
    .description("The original FlowFile will be routed upon success (2xx status codes). It will have new attributes detailing the "
      + "success of the request.")
    .build();

  public static final Relationship REL_RESPONSE = new Relationship.Builder()
    .name("Response")
    .description("A Response FlowFile will be routed upon success (2xx status codes). If the 'Output Response Regardless' property "
      + "is true then the response will be sent to this relationship regardless of the status code received.")
    .build();

  public static final Relationship REL_RETRY = new Relationship.Builder()
    .name("Retry")
    .description("The original FlowFile will be routed on any status code that can be retried (5xx status codes). It will have new "
      + "attributes detailing the request.")
    .build();

  public static final Relationship REL_NO_RETRY = new Relationship.Builder()
    .name("No Retry")
    .description("The original FlowFile will be routed on any status code that should NOT be retried (1xx, 3xx, 4xx status codes).  "
      + "It will have new attributes detailing the request.")
    .build();

  public static final Relationship REL_FAILURE = new Relationship.Builder()
    .name("Failure")
    .description("The original FlowFile will be routed on any type of connection failure, timeout or general exception. "
      + "It will have new attributes detailing the request.")
    .build();

  public static final Set<Relationship> RELATIONSHIPS = Collections.unmodifiableSet(new HashSet<>(Arrays.asList(
    REL_SUCCESS_REQ, REL_RESPONSE, REL_RETRY, REL_NO_RETRY, REL_FAILURE)));

  private List<PropertyDescriptor> descriptors;

  private Set<Relationship> relationships;
  private HttpClient client;

  @Override
  protected void init(final ProcessorInitializationContext context) {
    final List<PropertyDescriptor> descriptors = new ArrayList<>();
    descriptors.add(API_TOKEN);
    this.descriptors = Collections.unmodifiableList(descriptors);

    final Set<Relationship> relationships = new HashSet<>();
    relationships.add(REL_SUCCESS_REQ);
    relationships.add(REL_RESPONSE);
    relationships.add(REL_RETRY);
    relationships.add(REL_NO_RETRY);
    relationships.add(REL_FAILURE);
    this.relationships = Collections.unmodifiableSet(relationships);
  }

  @Override
  public Set<Relationship> getRelationships() {
    return this.relationships;
  }

  @Override
  public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
    return descriptors;
  }

  @OnScheduled
  public void createClient(final ProcessContext context) {
    Header header = new BasicHeader(
      HttpHeaders.AUTHORIZATION, "Bearer " + context.getProperty(API_TOKEN).getValue());
    List<Header> headers = Collections.singletonList(header);
    client = HttpClients.custom()
      .setDefaultHeaders(headers).build();
  }

  @OnUnscheduled
  @OnShutdown
  public void releaseClient() {
    client = null;
  }

  @Override
  public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {

    FlowFile requestFlowFile = session.get();

    List<String> urlsToDownload = new ArrayList<>();
    try (InputStream requestInputStream = session.read(requestFlowFile)) {
      JsonObject jsonObject = Json.createReader(requestInputStream).readObject();
      JsonArray files = jsonObject.getJsonArray("files");
      for (JsonValue file : files) {
        String url = ((JsonObject) file).getString("url_private_download");
        if (url != null) {
          urlsToDownload.add(url);
        }
      }
    } catch (IOException e) {
      session.transfer(requestFlowFile, REL_FAILURE);
      context.yield();
      return;
    }

    try {
      for (String downloadUrl : urlsToDownload) {
        HttpUriRequest request = RequestBuilder.get().setUri(downloadUrl).build();
        HttpResponse response = client.execute(request);
        int statusCode = response.getStatusLine().getStatusCode();
        if (inStatusCodeFamily(200, statusCode)) {
          FlowFile responseFlowFile = session.create();
          session.write(responseFlowFile, out -> response.getEntity().writeTo(out));
          session.transfer(responseFlowFile, REL_RESPONSE);
          session.transfer(requestFlowFile, REL_SUCCESS_REQ);
        }
        if (inStatusCodeFamily(100, statusCode)
          || inStatusCodeFamily(300, statusCode)
          || inStatusCodeFamily(400, statusCode)) {
          session.transfer(requestFlowFile, REL_NO_RETRY);
        } else if (inStatusCodeFamily(500, statusCode)) {
          session.transfer(requestFlowFile, REL_RETRY);
        }
      }
    } catch (IOException e) {
      session.transfer(requestFlowFile, REL_FAILURE);
    }
  }

  private boolean inStatusCodeFamily(int family, int statusCode) {
    return statusCode >= family && statusCode < family + 100;
  }

}
