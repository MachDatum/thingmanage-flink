package com.machdatum.thingmanage.model;

import org.apache.flink.table.api.DataTypes;

import java.util.List;


public class Table {
    public String Name;
    public List<Column> Columns;

    public  Table(String name, List<Column> columns){
        Name = name;
        Columns = columns;
    }

    public void setName(String Name){
        this.Name =Name;
        //    return null;
    }
    public String getName(){
        return this.Name;
    }
    public void setColumns(List<Column> columns){
        Columns = columns;
    }
    public List<Column> getColumns(){
        return this.Columns;
    }


}
