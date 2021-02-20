package com.machdatum.thingmanage.model;

import org.apache.flink.streaming.connectors.kafka.config.StartupMode;

import java.util.List;

public class KafkaConfiguration {
    public List<String> BootstrapServers;
    public String GroupId;
    public List<String> Topics;
    public StartupMode Startup;
    public void setBootstrapServers(List Servers){
        this.BootstrapServers = Servers;
    }
    public List<String> getBootstrapServers(){
        return this.BootstrapServers;
    }

    public void setGroupId(String GroupId){
        this.GroupId= GroupId;
    }
    public String getGroupId(){
        return this.GroupId;
    }

    public void setTopics(List Topics){
        this.Topics = Topics;
    }
    public List getTopics(){
        return this.Topics;
    }

    public void setStartupMode(StartupMode startup){
        this.Startup = startup;
    }
    public StartupMode getStartup(){
        return this.Startup;
    }
}
