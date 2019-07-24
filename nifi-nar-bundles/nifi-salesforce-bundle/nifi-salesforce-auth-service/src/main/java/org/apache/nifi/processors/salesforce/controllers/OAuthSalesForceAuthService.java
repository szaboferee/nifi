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
package org.apache.nifi.processors.salesforce.controllers;

import java.io.IOException;
import java.io.StringReader;
import java.util.Arrays;
import java.util.List;

import javax.json.Json;
import javax.json.JsonObject;
import javax.json.JsonReader;

import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyDescriptor.Builder;
import org.apache.nifi.components.Validator;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.processor.exception.ProcessException;

import okhttp3.FormBody;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;

public class OAuthSalesForceAuthService extends AbstractControllerService implements SalesForceAuthService {

  static final PropertyDescriptor LOGIN_URL = new Builder()
    .name("login-url")
    .displayName("Login Url")
    .description("")
    .defaultValue("https://login.salesforce.com")
    .addValidator(Validator.VALID)
    .required(true)
    .build();

  static final PropertyDescriptor USERNAME = new Builder()
    .name("username")
    .displayName("Username")
    .description("")
    .defaultValue("nifi@example.com")
    .addValidator(Validator.VALID)
    .required(true)
    .build();

  static final PropertyDescriptor PASSWORD = new Builder()
    .name("password")
    .displayName("Password")
    .description("")
    .defaultValue("asdf1234pVAOxKmCb75tW2Jg5WOEag5Jb")
    .addValidator(Validator.VALID)
    .required(true)
    .sensitive(true)
    .build();

  static final PropertyDescriptor CLIENT_ID = new Builder()
    .name("client-id")
    .displayName("Client Id")
    .description("")
    .defaultValue("3MVG96_7YM2sI9wR1i6b.qje2O5ugu96Okxs.C2LEiqpL6kRm1orfxl6dRFvA3hgXor2.JQ3ja3Ft0g4YC8Yl")
    .required(true)
    .addValidator(Validator.VALID)
    .sensitive(true)
    .build();

  static final PropertyDescriptor CLIENT_SECRET = new Builder()
    .name("client-secret")
    .displayName("Client Secret")
    .description("")
    .addValidator(Validator.VALID)
    .defaultValue("B169AF8063DD40AAF280D62C2CD624A98A959E432F5327904A44341AF2C6511A")
    .required(true)
    .sensitive(true)
    .build();

  private String loginUrl;
  private String username;
  private String password;
  private String clientId;
  private String clientSecret;

  private String instanceUrl;
  private String accessToken;


  @Override
  protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
    return Arrays.asList(LOGIN_URL, USERNAME, PASSWORD, CLIENT_ID, CLIENT_SECRET);
  }

  @Override
  public void renew() {
    accessToken = null;
    instanceUrl = null;
    requestAccessToken();
  }

  @Override
  public String getToken() {
    return accessToken;
  }

  @Override
  public String getInstanceUrl() {
    return instanceUrl;
  }

  @OnEnabled
  public void onEnabled(final ConfigurationContext configContext) {
    loginUrl = configContext.getProperty(LOGIN_URL).getValue();
    username = configContext.getProperty(USERNAME).getValue();
    password = configContext.getProperty(PASSWORD).getValue();
    clientId = configContext.getProperty(CLIENT_ID).getValue();
    clientSecret = configContext.getProperty(CLIENT_SECRET).getValue();
    renew();
  }

  private void requestAccessToken() {
      OkHttpClient okHttpClient = new OkHttpClient();

      FormBody formBody = new FormBody.Builder()
        .add("grant_type", "password")
        .add("client_id", clientId)
        .add("client_secret", clientSecret)
        .add("username", username)
        .add("password", password)
        .build();
      Request request = new Request.Builder()
        .url(loginUrl + "/services/oauth2/token")
        .post(formBody)
        .build();

      getLogger().debug("Calling login URL: " + request.url());
      try (Response response = okHttpClient.newCall(request).execute()) {
        getLogger().error("auth response: " + response);
        if (response.code() == 200) {
          if (response.body() != null) {
            String responseString = response.body().string();
            try (JsonReader reader = Json.createReader(new StringReader(responseString))) {
              JsonObject jsonObject = reader.readObject();
              accessToken = jsonObject.getString("access_token");
              instanceUrl = jsonObject.getString("instance_url");
            }
          }
        } else if (response.code() == 400) {
          if (response.body() != null) {
            throw new RuntimeException("Authentication error: " + response.body().string());
          }
        } else {
          throw new RuntimeException("Invalid response: " + response.toString());
        }

      } catch (IOException e) {
        throw new ProcessException("Error happened during token request.", e);
      }
    }
}
