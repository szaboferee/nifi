package org.apache.nifi.processors.salesforce;

import java.io.IOException;
import java.io.StringReader;
import java.io.StringWriter;

import javax.json.Json;
import javax.json.JsonObject;
import javax.json.JsonObjectBuilder;
import javax.json.JsonReader;
import javax.json.JsonWriter;
import javax.json.JsonWriterFactory;

import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;


@InputRequirement(Requirement.INPUT_ALLOWED)
public class BulkAPIProcessor extends AbstractSalesForceProcessor {

  static final PropertyDescriptor OPERATION = new PropertyDescriptor.Builder()
    .name("operation")
    .displayName("Operation")
    .description("Bulk API operation")
    .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
    .build();

  static final PropertyDescriptor SOBJECT_NAME = new PropertyDescriptor.Builder()
    .name("sobject-name")
    .displayName("SObject Name")
    .description("")
    .required(true)
    .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
    .build();


  @Override
  public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {

    String operation = context.getProperty(OPERATION).getValue();
    String objectName = context.getProperty(SOBJECT_NAME).getValue();

    JsonObjectBuilder objectBuilder = Json.createObjectBuilder();
    objectBuilder.add("operation", operation);
    objectBuilder.add("object", objectBuilder);
    objectBuilder.add("contentType", "JSON");
    JsonObject jsonObject = objectBuilder.build();
    StringWriter writer = new StringWriter();
    Json.createWriter(writer).writeObject(jsonObject);
    String s = writer.getBuffer().toString();



    //create job

    String jobId = createJob(context, s);

    //create batch

  }

  private String createJob(ProcessContext context, String jobContent) {

    String s = getVersionedAsyncPath("v46", "/job");

    String response = doPostRequest(s, jobContent);

    JsonReader reader = Json.createReader(new StringReader(response));
    String id = reader.readObject().getString("id");

    System.out.println(id);

    return id;
  }
}
