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

import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.ReadsAttribute;
import org.apache.nifi.annotation.behavior.ReadsAttributes;
import org.apache.nifi.annotation.configuration.DefaultSchedule;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.scheduling.SchedulingStrategy;


@InputRequirement(Requirement.INPUT_REQUIRED)
@ReadsAttributes({
  @ReadsAttribute(attribute = "salesforce.attributes.url", description = "TODO")
})
@DefaultSchedule(strategy = SchedulingStrategy.TIMER_DRIVEN, period = "1 day")
@Tags({"salesforce", "sobject"})
public class FetchSObject extends AbstractSalesForceProcessor {

  @Override
  public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
    FlowFile parent = session.get();
    if (parent == null) {
      context.yield();
      return;
    }

    String url = parent.getAttribute("salesforce.attributes.url");

    FlowFile flowFile = session.create(parent);

    String response = doGetRequest(url);
    session.write(flowFile, out -> out.write(response.getBytes()));
    session.transfer(flowFile, REL_SUCCESS);
    session.remove(parent);
  }
}
