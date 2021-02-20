package com.machdatum.thingmanage.model;

import java.util.List;

public class FlinkProcess {
    public KafkaConfiguration Source;
    public Table Schema;

    public List<Object> Transformations;
    public KafkaConfiguration Sink;
}
