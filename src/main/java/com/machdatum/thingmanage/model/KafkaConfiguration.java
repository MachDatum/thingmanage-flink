package com.machdatum.thingmanage.model;

import org.apache.flink.streaming.connectors.kafka.config.StartupMode;

import java.util.List;

public class KafkaConfiguration {
    public List<String> BootstrapServers;
    public String GroupId;
    public List<String> Topics;
    public StartupMode Startup;
}
