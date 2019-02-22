package org.apache.nifi.processors.slack.controllers;

import java.util.function.Consumer;

import org.apache.nifi.controller.ControllerService;
import org.apache.nifi.processor.Processor;

public interface SlackConnectionService extends ControllerService {
  void registerProcessor(final Processor processor, Consumer<String> messageHandler);

  boolean isProcessorRegistered(final Processor processor);

  void deregisterProcessor(final Processor processor);

  void sendMessage(final String message);
}
