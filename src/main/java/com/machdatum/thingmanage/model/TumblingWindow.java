package com.machdatum.thingmanage.model;

import java.util.List;

public class TumblingWindow {
    public String Name;
    public String Over;
    public String On;
    public String As;
    public List<String> GroupBy;
    public List<String> Select;
    public List<Field> Fields;

    public TumblingWindow(String name, String over, String on, String as, List<String> groupBy, List<String> select){
        Name = name;
        Over = over;
        On = on;
        As = as;
        GroupBy = groupBy;
        Select = select;
    }
}
