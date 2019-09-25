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
import java.io.StringReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import javax.json.Json;
import javax.json.JsonArray;
import javax.json.JsonArrayBuilder;
import javax.json.JsonException;
import javax.json.JsonObject;
import javax.json.JsonReader;
import javax.json.JsonString;
import javax.json.JsonValue;

import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.PrimaryNodeOnly;
import org.apache.nifi.annotation.behavior.Stateful;
import org.apache.nifi.annotation.behavior.TriggerSerially;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.state.Scope;
import org.apache.nifi.components.state.StateManager;
import org.apache.nifi.components.state.StateMap;
import org.apache.nifi.expression.AttributeExpression;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;

@InputRequirement(Requirement.INPUT_FORBIDDEN)
@Stateful(scopes = Scope.CLUSTER, description = "TODO")
@Tags({"salesforce", "sobject"})
@PrimaryNodeOnly
@TriggerSerially
public class QuerySOQL extends AbstractSalesForceProcessor {

  public static final String INITIAL_MAX_VALUE_PROP_START = "initial.maxvalue.";
  // The delimiter to use when referencing qualified names (such as table@!@column in the state map)
  protected static final String NAMESPACE_DELIMITER = "@!@";

  static final PropertyDescriptor OBJECT_NAME = new PropertyDescriptor.Builder()
      .name("object-name")
      .displayName("Object name")
      .description("The name of object to be queried.")
      .required(true)
      .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
      .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
      .build();

  static final PropertyDescriptor FIELD_NAMES = new PropertyDescriptor.Builder()
      .name("field_names")
      .displayName("Column names")
      .description("A coma-separated list of column names to be used in the query.")
      .required(true)
      .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
      .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
      .build();

  static final PropertyDescriptor WHERE_CLAUSE = new PropertyDescriptor.Builder()
      .name("where_clause")
      .displayName("Additional WHERE clause")
      .description("A custom clause to be added in the WHERE condition when building SOQL queries.")
      .required(false)
      .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
      .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
      .build();

  static final PropertyDescriptor MAX_VALUE_COLUMN_NAMES = new PropertyDescriptor.Builder()
      .name("Maximum-value Columns")
      .description("A comma-separated list of column names. The processor will keep track of the maximum value "
          + "for each column that has been returned since the processor started running. Using multiple columns implies an order "
          + "to the column list, and each column's values are expected to increase more slowly than the previous columns' values. Thus, "
          + "using multiple columns implies a hierarchical structure of columns, which is usually used for partitioning tables.")
      .required(false)
      .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
      .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
      .build();

  static final PropertyDescriptor MAX_ROWS_PER_FLOW_FILE = new PropertyDescriptor.Builder()
      .name("max-rows")
      .displayName("Max Rows Per Flow File")
      .description("The maximum number of result rows that will be included in a single FlowFile. This will allow you to break up very large "
          + "result sets into multiple FlowFiles. If the value specified is zero, then all rows are returned in a single FlowFile.")
      .defaultValue("0")
      .required(true)
      .addValidator(StandardValidators.NON_NEGATIVE_INTEGER_VALIDATOR)
      .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
      .build();

  static final PropertyDescriptor INCLUDE_DELETED = new PropertyDescriptor.Builder()
      .name("include-deleted")
      .displayName("Include deleted (QueryAll)")
      .description("Executes the specified SOQL query. Unlike the Query resource, QueryAll will return records " +
          "that have been deleted because of a merge or delete. QueryAll will also return information " +
          "about archived Task and Event records. QueryAll is available in API version 29.0 and later.")
      .required(true)
      .allowableValues("true", "false")
      .defaultValue("false")
      .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
      .build();

  private String path;
  private Long maxRowsPerFlowFile;
  private String fieldNames;
  private String objectName;
  private AtomicBoolean metaDataIsSet = new AtomicBoolean(false);
  private String apiVersion;
  private Map<String, String> fieldTypeMap;
  // A Map (name to value) of initial maximum-value properties, filled at schedule-time and used at trigger-time
  protected Map<String, String> maxValueProperties;
  private List<String> maxValueColumnNames;

  @Override
  protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
    List<PropertyDescriptor> propertyDescriptors = new ArrayList<>(super.getSupportedPropertyDescriptors());
    propertyDescriptors.addAll(Arrays.asList(OBJECT_NAME, FIELD_NAMES, WHERE_CLAUSE, MAX_VALUE_COLUMN_NAMES, MAX_ROWS_PER_FLOW_FILE, INCLUDE_DELETED));
    return propertyDescriptors;
  }

  @Override
  protected PropertyDescriptor getSupportedDynamicPropertyDescriptor(final String propertyDescriptorName) {
    return new PropertyDescriptor.Builder()
        .name(propertyDescriptorName)
        .required(false)
        .addValidator(StandardValidators.createAttributeExpressionLanguageValidator(AttributeExpression.ResultType.STRING, true))
        .addValidator(StandardValidators.ATTRIBUTE_KEY_PROPERTY_NAME_VALIDATOR)
        .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
        .dynamic(true)
        .build();
  }

  @OnScheduled
  public void setup(ProcessContext context) {
    apiVersion = context.getProperty(API_VERSION).evaluateAttributeExpressions().getValue();
    Boolean includeDeleted = context.getProperty(INCLUDE_DELETED).asBoolean();
    path = getVersionedPath(apiVersion, includeDeleted ? "/queryAll" : "/query");
    maxRowsPerFlowFile = context.getProperty(MAX_ROWS_PER_FLOW_FILE).evaluateAttributeExpressions().asLong();
    fieldNames = context.getProperty(FIELD_NAMES).evaluateAttributeExpressions().getValue();
    objectName = context.getProperty(OBJECT_NAME).evaluateAttributeExpressions().getValue();

    maxValueProperties = getDefaultMaxValueProperties(context);
    if (context.getProperty(MAX_VALUE_COLUMN_NAMES).isSet()) {
      String value = context.getProperty(MAX_VALUE_COLUMN_NAMES).evaluateAttributeExpressions().getValue();
      maxValueColumnNames = Arrays.stream(value.split("\\s*,\\s*")).collect(Collectors.toList());
    }

  }

  @Override
  public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
    if (!metaDataIsSet.get()) {
      setMetaData();
    }
    StateManager stateManager = context.getStateManager();
    StateMap stateMap;
    try {
      stateMap = stateManager.getState(Scope.CLUSTER);
    } catch (IOException e) {
      getLogger().error("Failed to retrieve observed maximum values from the State Manager. Will not perform "
          + "query until this is accomplished.", e);
      context.yield();
      return;
    }

    // Make a mutable copy of the current state property map. This will be updated by the result row callback, and eventually
    // set as the current state map (after the session has been committed)
    final Map<String, String> statePropertyMap = new HashMap<>(stateMap.toMap());

    //If an initial max value for column(s) has been specified using properties, and this column is not in the state manager, sync them to the state property map
    for (final Map.Entry<String, String> maxProp : maxValueProperties.entrySet()) {
      String maxPropKey = maxProp.getKey().toLowerCase();
      String fullyQualifiedMaxPropKey = objectName.toLowerCase() + NAMESPACE_DELIMITER + maxPropKey;
      statePropertyMap.putIfAbsent(fullyQualifiedMaxPropKey, maxProp.getValue());
    }


    String query = getQuery(context, statePropertyMap);

    HashMap<String, String> queryParams = new HashMap<>();
    queryParams.put("q", query);
    String result = doGetRequest(path, queryParams);

    parseResultsToFlowFiles(context, session, result, statePropertyMap);

    try {
      // Update the state
      stateManager.setState(statePropertyMap, Scope.CLUSTER);
    } catch (IOException ioe) {
      getLogger().error("{} failed to update State Manager, maximum observed values will not be recorded", new Object[]{this, ioe});
    }
  }

  private synchronized void setMetaData() {
    String metaDataJson = doGetRequest(getVersionedPath(apiVersion, "/sobjects/" + objectName + "/describe"));
    fieldTypeMap = new HashMap<>();
    JsonReader reader = Json.createReader(new StringReader(metaDataJson));
    reader.readObject().getJsonArray("fields").forEach(jsonValue -> {
      JsonObject object = (JsonObject) jsonValue;
      fieldTypeMap.put(object.getString("name").toLowerCase(), object.getString("type"));
    });

    metaDataIsSet.set(true);
  }


  private String getQuery(ProcessContext context, Map<String, String> stateMap) {
    List<String> whereClauses = new ArrayList<>();
    StringBuilder query = new StringBuilder();
    query.append("SELECT ");
    query.append(fieldNames);
    query.append(" FROM ");
    query.append(objectName);


    if (stateMap != null && !stateMap.isEmpty() && maxValueColumnNames != null) {
      IntStream.range(0, maxValueColumnNames.size()).forEach((index) -> {
        String colName = maxValueColumnNames.get(index);
        String maxValueKey = objectName.toLowerCase() + NAMESPACE_DELIMITER + colName.toLowerCase();
        String maxValue = stateMap.get(maxValueKey);
        if (!maxValue.isEmpty()) {
          // Add a condition for the WHERE clause
          whereClauses.add(colName + (index == 0 ? " > " : " >= ") + getQuotedValue(maxValue, colName.toLowerCase()));
        }
      });
    }

    if (context.getProperty(WHERE_CLAUSE).isSet()) {
      whereClauses.add(context.getProperty(WHERE_CLAUSE).evaluateAttributeExpressions().getValue());
    }

    if (!whereClauses.isEmpty()) {
      query.append(" WHERE ");
      query.append(String.join(" AND ", whereClauses));
    }

    if (maxValueColumnNames != null) {
      query.append(" ORDER BY ");
      query.append(String.join(",", maxValueColumnNames));
      query.append(" ASC");
    }

    return query.toString();
  }

  private String getQuotedValue(String value, String colName) {
    return needQuotes(fieldTypeMap.get(colName))
        ? "'" + value + "'"
        : value;
  }

  private void parseResultsToFlowFiles(ProcessContext context, ProcessSession session, String result, Map<String, String> statePropertyMap) {
    Map<String, String> newState = new HashMap<>(statePropertyMap);
    ResultWriter resultWriter = new ResultWriter(session);
    parseResult(resultWriter, result, newState);
    resultWriter.transfer();
    statePropertyMap.putAll(newState);
  }

  private void parseResult(ResultWriter resultWriter, String result, Map<String, String> newState) {
    if (result.length() > 0) {
      try (JsonReader reader = Json.createReader(new StringReader(result))) {
        JsonObject jsonObject = reader.readObject();
        JsonArray records = jsonObject.getJsonArray("records");
        JsonObject lastRecord = null;
        for (JsonValue record : records) {
          resultWriter.add(record);
          lastRecord = (JsonObject) record;
        }
        if (lastRecord != null) {

          JsonObject finalLastRecord = lastRecord;
          if (maxValueColumnNames != null) {
            maxValueColumnNames.forEach(s -> {
              newState.put(objectName.toLowerCase() + NAMESPACE_DELIMITER + s.toLowerCase(), getStringValue(finalLastRecord.get(s)));
            });
          }
        }
        if (!jsonObject.getBoolean("done")) {
          String nextRecordsResult = doGetRequest(jsonObject.getString("nextRecordsUrl"));
          parseResult(resultWriter, nextRecordsResult, newState);
        }
      } catch (JsonException e) {
        getLogger().error("Error while parsing result: [" + result + "]", e);
      }
    }
  }

  private String getStringValue(JsonValue jsonValue) {
    String result;
    if (jsonValue instanceof JsonString) {
      result = ((JsonString) jsonValue).getString();
    } else {
      result = jsonValue.toString();
    }

    return result;
  }

  private boolean needQuotes(String fieldType) {
    Set<String> typesWithoutQuote = new HashSet<>(Arrays.asList(
        "date",
        "double",
        "datetime",
        "int",
        "boolean",
        "currency",
        "percent"
    ));

    return !typesWithoutQuote.contains(fieldType);
  }

  private Map<String, String> getDefaultMaxValueProperties(final ProcessContext context) {
    final Map<String, String> defaultMaxValues = new HashMap<>();
    context.getProperties().forEach((propertyDescriptor, value) -> {
      final String key = propertyDescriptor.getName();
      if (key.startsWith(INITIAL_MAX_VALUE_PROP_START)) {
        defaultMaxValues.put(key.substring(INITIAL_MAX_VALUE_PROP_START.length()), context.getProperty(propertyDescriptor).evaluateAttributeExpressions().getValue());
      }
    });

    return defaultMaxValues;
  }

  private class ResultWriter {
    private ProcessSession session;
    private List<JsonValue> records = new LinkedList<>();
    private List<FlowFile> flowfiles = new LinkedList<>();

    public ResultWriter(ProcessSession session) {
      this.session = session;
    }

    void add(JsonValue record) {
      rotateIfNeeded();
      records.add(record);
    }

    private void rotateIfNeeded() {
      if (maxRowsPerFlowFile > 0 && records.size() == maxRowsPerFlowFile) {
        addRecordToFlowFile();
      }
    }

    private void addRecordToFlowFile() {
      JsonArrayBuilder resultBuilder = Json.createArrayBuilder();
      records.forEach(resultBuilder::add);
      JsonArray jsonResult = resultBuilder.build();

      FlowFile flowFile = session.create();
      session.write(flowFile, out -> out.write(jsonResult.toString().getBytes()));
      session.putAttribute(flowFile, "salesforce.attributes.type", objectName);
      flowfiles.add(flowFile);

      records.clear();
    }

    void transfer() {
      if (!records.isEmpty()) {
        addRecordToFlowFile();
      }
      if (!flowfiles.isEmpty()) {
        session.transfer(flowfiles, REL_SUCCESS);
      }
    }
  }
}
