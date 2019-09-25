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
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.Validator;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processors.salesforce.controllers.SalesForceAuthService;

import okhttp3.HttpUrl;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.OkHttpClient.Builder;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;

public abstract class AbstractSalesForceProcessor extends AbstractProcessor {

  static final PropertyDescriptor AUTH_SERVICE = new PropertyDescriptor.Builder()
    .name("auth-service")
    .displayName("Auth service")
    .identifiesControllerService(SalesForceAuthService.class)
    .required(true)
    .build();

  static final PropertyDescriptor API_VERSION = new PropertyDescriptor.Builder()
    .name("api-version")
    .displayName("API Version")
    .description("")
    .required(true)
    .addValidator(Validator.VALID)
      .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
    .defaultValue("v46.0")
    .build();

  static final Relationship REL_SUCCESS = new Relationship.Builder()
    .name("success")
    .description("")
    .build();

  private SalesForceAuthService authService;


  @Override
  protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
    return Arrays.asList(AUTH_SERVICE, API_VERSION);
  }

  @Override
  public Set<Relationship> getRelationships() {
    return new HashSet<>(Arrays.asList(REL_SUCCESS));
  }

  @OnScheduled
  public void onScheduled(ProcessContext context) {
    authService = context.getProperty(AUTH_SERVICE).asControllerService(SalesForceAuthService.class);
  }

  protected String doGetRequest(String url) {
    return doGetRequest(url, Collections.emptyMap());
  }

  protected String doGetRequest(String url, Map<String, String> queryParams) {
    OkHttpClient client = new Builder().build();

    try (Response response = client.newCall(buildGetRequest(url, queryParams)).execute()) {
      getLogger().trace("Processor response: " + response);
      if (response.code() == 401) {
        authService.renew();
        try (Response response2 = client.newCall(buildGetRequest(url, queryParams)).execute()) {
          return handleResponse(response2);
        }
      }

      // TODO handle limit exceeded error
      return handleResponse(response);
    } catch (Exception e) {
      throw new ProcessException("Error while handling Response ", e);
    }
  }

  protected String doPostRequest(String url, String body) {
    return doPostRequest(url, Collections.emptyMap(), body);
  }

  protected String doPostRequest(String url, Map<String, String> queryParams, String body) {
    OkHttpClient client = new Builder().build();

    try (Response response = client.newCall(buildPostRequest(url, queryParams, body)).execute()) {
      getLogger().trace("Processor response: " + response);
      if (response.code() == 401) {
        authService.renew();
        try (Response response2 = client.newCall(buildPostRequest(url, queryParams, body)).execute()) {
          return handleResponse(response2);
        }
      }

      // TODO handle limit exceeded error
      return handleResponse(response);
    } catch (Exception e) {
      throw new ProcessException("Error while handling Response ", e);
    }
  }


  private String handleResponse(Response response) throws IOException {
    if (response.code() != 200) {
      throw new ProcessException("Invalid response" +
        " Code: " + response.code() +
        " Message: " + response.message() +
        " Body: " + (response.body() == null ? null : response.body().string())
      );
    }

    //TODO failure relationship
    return Objects.requireNonNull(response.body()).string();
  }

  private Request buildGetRequest(String url, Map<String, String> queryParams) {
    HttpUrl.Builder builder = HttpUrl.get(getUrl(url)).newBuilder();
    queryParams.forEach(builder::addQueryParameter);
    HttpUrl httpUrl = builder.build();

    getLogger().debug("Salesforce url called: {}", new Object[]{httpUrl.toString()});

    return new Request.Builder()
      .url(httpUrl)
      .addHeader("Authorization", "Bearer " + getToken())
      .get()
      .build();
  }

  private Request buildPostRequest(String url, Map<String, String> queryParams, String body) {
    HttpUrl.Builder builder = HttpUrl.get(getUrl(url)).newBuilder();
    queryParams.forEach(builder::addQueryParameter);
    HttpUrl httpUrl = builder.build();

    getLogger().debug("Salesforce url called: {}", new Object[]{httpUrl.toString()});

    return new Request.Builder()
      .url(httpUrl)
      .addHeader("Authorization", "Bearer " + getToken())
      .post(RequestBody.create(MediaType.get("application/json"), body))
      .build();
  }

  private String getToken() {
    return authService.getToken();
  }

  private String getUrl(String url) {
    return authService.getInstanceUrl() + url;
  }

  String getVersionedPath(String version, String url) {
    return "/services/data/" + version + url;
  }

  String getVersionedAsyncPath(String version, String url) {
    return "/services/async/" + version + url;
  }
}
