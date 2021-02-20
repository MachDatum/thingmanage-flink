package com.machdatum.thingmanage.model;

import org.apache.flink.table.api.DataTypes;

import java.util.List;


public class Table {
    public String Name;
    public List<Column> Columns;

    public void setName(String Name){
        this.Name =Name;
        //    return null;
    }
    public String getName(){
        return this.Name;
    }
    public void setColumns(List<Column> column){
        /*column.forEach( temp_column ->{
        Column target = new Column();
        target.setSource(temp_column.getSource());
        target.setType(temp_column.getType());
        target.setAs(temp_column.getAs());
        Columns.add(target);
        });*/
        Columns = column;
        //return null;
    }
    public List<Column> getColumns(){
        return this.Columns;
    }
}
