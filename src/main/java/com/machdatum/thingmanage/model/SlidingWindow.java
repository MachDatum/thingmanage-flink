package com.machdatum.thingmanage.model;

import java.util.List;

public class SlidingWindow extends TumblingWindow{
    public String Every;


    public SlidingWindow(String name, String over, String on, String as, List<String> groupBy, List<String> select, String every)
    {
        super(name, over, on, as, groupBy, select);
        Every  = every;
    }
}
