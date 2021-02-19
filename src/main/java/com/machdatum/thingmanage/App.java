package com.machdatum.thingmanage;

import com.squareup.javapoet.*;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import javax.lang.model.element.Modifier;

import static org.apache.flink.table.api.Expressions.$;

public class App 
{
    private static ClassName tableName = ClassName.get("org.apache.flink.table.api", "Table");
    private static ClassName envName = ClassName.get("org.apache.flink.table.bridge.java", "StreamTableEnvironment");

    private  static  final MethodSpec GenerateFilter(String name, String filter){
        return MethodSpec.methodBuilder(name)
                .addModifiers(Modifier.PRIVATE, Modifier.FINAL, Modifier.STATIC)
                .returns(tableName)
                .addParameter(envName, "tEnv")
                .addParameter(tableName, "source")
                .addStatement("$T table = $L.where($S)", tableName, "source", filter)
                .addStatement("$L.registerTable($S, $L)", "tEnv", name, "table")
                .addStatement("return table")
                .build();
    }

    public static void main( String[] args )
    {
        StreamTableEnvironment env = null;
        Table t = null;

        ClassName tableName = ClassName.get("org.apache.flink.table.api", "Table");
        ClassName envName = ClassName.get("org.apache.flink.table.bridge.java", "StreamTableEnvironment");

        MethodSpec KafkaSource1 = MethodSpec.methodBuilder("KafkaSource1")
                .addModifiers(Modifier.PRIVATE, Modifier.FINAL, Modifier.STATIC)
                .returns(tableName)
                .addParameter(envName, "tEnv")
                .build();

        MethodSpec Filter1 = GenerateFilter("Filter1", "IsNYC");
        MethodSpec Filter2 = GenerateFilter("Filter2", "IsW");

        MethodSpec Select1 = MethodSpec.methodBuilder("Select1")
                .addModifiers(Modifier.PRIVATE, Modifier.FINAL, Modifier.STATIC)
                .returns(tableName)
                .addParameter(envName, "tEnv")
                .addParameter(tableName, Filter1.name)
                .build();

        MethodSpec Window1 = MethodSpec.methodBuilder("Window1")
                .addModifiers(Modifier.PRIVATE, Modifier.FINAL, Modifier.STATIC)
                .returns(tableName)
                .addParameter(envName, "tEnv")
                .addParameter(tableName, Select1.name)
                .build();

        MethodSpec Main = MethodSpec.methodBuilder("main")
                .addModifiers(Modifier.PUBLIC, Modifier.STATIC)
                .returns(void.class)
                .addParameter(String[].class, "args")
                .build();

        TypeSpec MainClass = TypeSpec.classBuilder("Main")
                .addModifiers(Modifier.PUBLIC, Modifier.FINAL)
                .addMethod(Main)
                .addMethod(KafkaSource1)
                .addMethod(Filter1)
                .addMethod(Filter2)
                .addMethod(Select1)
                .addMethod(Window1)
                .build();

        JavaFile file = JavaFile.builder("com.machdatum.thingmanage", MainClass)
                .build();
        System.out.println(file.toString());
    }
}
