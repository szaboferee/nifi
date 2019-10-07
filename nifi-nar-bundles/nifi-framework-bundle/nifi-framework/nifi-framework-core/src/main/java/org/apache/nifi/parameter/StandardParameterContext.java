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
package org.apache.nifi.parameter;

import org.apache.nifi.authorization.Resource;
import org.apache.nifi.authorization.resource.Authorizable;
import org.apache.nifi.authorization.resource.ResourceFactory;
import org.apache.nifi.authorization.resource.ResourceType;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.ComponentNode;
import org.apache.nifi.controller.ProcessorNode;
import org.apache.nifi.controller.PropertyConfiguration;
import org.apache.nifi.controller.service.ControllerServiceNode;
import org.apache.nifi.controller.service.ControllerServiceState;
import org.apache.nifi.groups.ProcessGroup;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class StandardParameterContext implements ParameterContext {
    private static final Logger logger = LoggerFactory.getLogger(StandardParameterContext.class);

    private final String id;
    private final ParameterReferenceManager parameterReferenceManager;
    private final Authorizable parentAuthorizable;

    private String name;
    private long version = 0L;
    private final Map<ParameterDescriptor, Parameter> parameters = new LinkedHashMap<>();
    private volatile String description;

    private final ReadWriteLock rwLock = new ReentrantReadWriteLock();
    private final Lock readLock = rwLock.readLock();
    private final Lock writeLock = rwLock.writeLock();


    public StandardParameterContext(final String id, final String name, final ParameterReferenceManager parameterReferenceManager, final Authorizable parentAuthorizable) {
        this.id = Objects.requireNonNull(id);
        this.name = Objects.requireNonNull(name);
        this.parameterReferenceManager = parameterReferenceManager;
        this.parentAuthorizable = parentAuthorizable;
    }

    @Override
    public String getIdentifier() {
        return id;
    }

    public String getName() {
        readLock.lock();
        try {
            return name;
        } finally {
            readLock.unlock();
        }
    }

    public void setName(final String name) {
        writeLock.lock();
        try {
            this.version++;
            this.name = name;
        } finally {
            writeLock.unlock();
        }
    }

    @Override
    public void setDescription(String description) {
        this.description = description;
    }

    @Override
    public String getDescription() {
        return description;
    }

    public void setParameters(final Map<String, Parameter> updatedParameters) {
        writeLock.lock();
        try {
            this.version++;
            verifyCanSetParameters(updatedParameters);

            boolean changeAffectingComponents = false;
            for (final Map.Entry<String, Parameter> entry : updatedParameters.entrySet()) {
                final String parameterName = entry.getKey();
                final Parameter parameter = entry.getValue();

                if (parameter == null) {
                    final ParameterDescriptor parameterDescriptor = new ParameterDescriptor.Builder().name(parameterName).build();
                    parameters.remove(parameterDescriptor);
                    changeAffectingComponents = true;
                } else {
                    final Parameter oldParameter = parameters.put(parameter.getDescriptor(), parameter);
                    if (oldParameter == null || !Objects.equals(oldParameter.getValue(), parameter.getValue())) {
                        changeAffectingComponents = true;
                    }
                }
            }

            if (changeAffectingComponents) {
                for (final ProcessGroup processGroup : parameterReferenceManager.getProcessGroupsBound(this)) {
                    try {
                        processGroup.onParameterContextUpdated();
                    } catch (final Exception e) {
                        logger.error("Failed to notify {} that Parameter Context was updated", processGroup, e);
                    }
                }
            }
        } finally {
            writeLock.unlock();
        }
    }

    @Override
    public long getVersion() {
        readLock.lock();
        try {
            return version;
        } finally {
            readLock.unlock();
        }
    }

    public Optional<Parameter> getParameter(final String parameterName) {
        readLock.lock();
        try {
            final ParameterDescriptor descriptor = new ParameterDescriptor.Builder().name(parameterName).build();
            return getParameter(descriptor);
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public boolean isEmpty() {
        readLock.lock();
        try {
            return parameters.isEmpty();
        } finally {
            readLock.unlock();
        }
    }

    public Optional<Parameter> getParameter(final ParameterDescriptor parameterDescriptor) {
        readLock.lock();
        try {
            return Optional.ofNullable(parameters.get(parameterDescriptor));
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public Map<ParameterDescriptor, Parameter> getParameters() {
        readLock.lock();
        try {
            return new LinkedHashMap<>(parameters);
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public ParameterReferenceManager getParameterReferenceManager() {
        return parameterReferenceManager;
    }

    @Override
    public void verifyCanSetParameters(final Map<String, Parameter> updatedParameters) {
        // Ensure that the updated parameters will not result in changing the sensitivity flag of any parameter.
        for (final Map.Entry<String, Parameter> entry : updatedParameters.entrySet()) {
            final String parameterName = entry.getKey();
            final Parameter parameter = entry.getValue();
            if (parameter == null) {
                // parameter is being deleted.
                validateReferencingComponents(parameterName, null,"remove");
                continue;
            }

            if (!Objects.equals(parameterName, parameter.getDescriptor().getName())) {
                throw new IllegalArgumentException("Parameter '" + parameterName + "' was specified with the wrong key in the Map");
            }

            validateSensitiveFlag(parameter);
            validateReferencingComponents(parameterName, parameter, "update");
        }
    }

    private void validateSensitiveFlag(final Parameter updatedParameter) {
        final ParameterDescriptor updatedDescriptor = updatedParameter.getDescriptor();
        final Parameter existingParameter = parameters.get(updatedDescriptor);

        if (existingParameter == null) {
            return;
        }

        final ParameterDescriptor existingDescriptor = existingParameter.getDescriptor();
        if (existingDescriptor.isSensitive() != updatedDescriptor.isSensitive() && updatedParameter.getValue() != null) {
            final String existingSensitiveDescription = existingDescriptor.isSensitive() ? "sensitive" : "not sensitive";
            final String updatedSensitiveDescription = updatedDescriptor.isSensitive() ? "sensitive" : "not sensitive";

            throw new IllegalStateException("Cannot update Parameters because doing so would change Parameter '" + existingDescriptor.getName() + "' from " + existingSensitiveDescription
                + " to " + updatedSensitiveDescription);
        }
    }


    private void validateReferencingComponents(final String parameterName, final Parameter parameter, final String parameterAction) {
        for (final ProcessorNode procNode : parameterReferenceManager.getProcessorsReferencing(this, parameterName)) {
            if (procNode.isRunning()) {
                throw new IllegalStateException("Cannot " + parameterAction + " parameter '" + parameterName + "' because it is referenced by " + procNode + ", which is currently running");
            }

            if (parameter != null) {
                validateParameterSensitivity(parameter, procNode);
            }
        }

        for (final ControllerServiceNode serviceNode : parameterReferenceManager.getControllerServicesReferencing(this, parameterName)) {
            final ControllerServiceState serviceState = serviceNode.getState();
            if (serviceState != ControllerServiceState.DISABLED) {
                throw new IllegalStateException("Cannot " + parameterAction + " parameter '" + parameterName + "' because it is referenced by "
                    + serviceNode + ", which currently has a state of " + serviceState);
            }

            if (parameter != null) {
                validateParameterSensitivity(parameter, serviceNode);
            }
        }
    }

    private void validateParameterSensitivity(final Parameter parameter, final ComponentNode componentNode) {
        final String paramName = parameter.getDescriptor().getName();

        for (final Map.Entry<PropertyDescriptor, PropertyConfiguration> entry :  componentNode.getProperties().entrySet()) {
            final PropertyConfiguration configuration = entry.getValue();
            if (configuration == null) {
                continue;
            }

            for (final ParameterReference reference : configuration.getParameterReferences()) {
                if (parameter.getDescriptor().getName().equals(reference.getParameterName())) {
                    final PropertyDescriptor propertyDescriptor = entry.getKey();
                    if (propertyDescriptor.isSensitive() && !parameter.getDescriptor().isSensitive()) {
                        throw new IllegalStateException("Cannot add Parameter with name '" + paramName + "' unless that Parameter is Sensitive because a Parameter with that name is already " +
                            "referenced from a Sensitive Property");
                    }

                    if (!propertyDescriptor.isSensitive() && parameter.getDescriptor().isSensitive()) {
                        throw new IllegalStateException("Cannot add Parameter with name '" + paramName + "' unless that Parameter is Not Sensitive because a Parameter with that name is already " +
                            "referenced from a Property that is not Sensitive");
                    }
                }
            }
        }
    }

    @Override
    public Authorizable getParentAuthorizable() {
        return new Authorizable() {
            @Override
            public Authorizable getParentAuthorizable() {
                return parentAuthorizable;
            }

            @Override
            public Resource getResource() {
                return ResourceFactory.getParameterContextsResource();
            }
        };
    }

    @Override
    public Resource getResource() {
        return ResourceFactory.getComponentResource(ResourceType.ParameterContext, getIdentifier(), getName());
    }
}
