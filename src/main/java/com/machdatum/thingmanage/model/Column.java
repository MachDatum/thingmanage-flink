package com.machdatum.thingmanage.model;

public class Column{
    public String Source;
    public String Type;
    public String As;

    public  Column(String source, String type, String as){
        Source = source;
        Type = type;
        As = as;
    }

    public String getSource(){
        return this.Source;
    }
    public void setSource(String Source){
        this.Source = Source;
    }
    public String getType(){
        return this.Type;
    }
    public void setType(String Type){
        this.Type = Type;
    }
    public String getAs(){
        return this.As;
    }
    public void setAs(String As){
        this.As = As;
    }
}

