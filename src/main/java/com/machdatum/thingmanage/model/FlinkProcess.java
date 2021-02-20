package com.machdatum.thingmanage.model;

import java.util.List;

public class FlinkProcess {
    public KafkaConfiguration Source;
    public Table Schema;
    public List<Object> Transformations;
    public KafkaConfiguration Sink;

    public FlinkProcess(KafkaConfiguration source, Table schema, List<Object> transformations, KafkaConfiguration sink){
        Source = source;
        Schema = schema;
        Transformations = transformations;
        Sink = sink;
    }
}
