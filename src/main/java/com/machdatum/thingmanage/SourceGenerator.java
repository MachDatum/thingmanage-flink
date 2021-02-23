package com.machdatum.thingmanage;

import com.machdatum.thingmanage.model.*;
import com.squareup.javapoet.ClassName;
import com.squareup.javapoet.JavaFile;
import com.squareup.javapoet.MethodSpec;
import com.squareup.javapoet.TypeSpec;
import freemarker.template.Configuration;
import freemarker.template.Template;

import javax.lang.model.element.Modifier;
import java.io.File;
import java.io.OutputStreamWriter;
import java.io.StringWriter;
import java.io.Writer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static freemarker.template.Configuration.DEFAULT_INCOMPATIBLE_IMPROVEMENTS;

public class SourceGenerator {
    private static ClassName tableName = ClassName.get("org.apache.flink.table.api", "Table");
    private static ClassName envName = ClassName.get("org.apache.flink.streaming.api.environment", "StreamExecutionEnvironment");
    private static ClassName envSettingsName = ClassName.get("org.apache.flink.table.api", "EnvironmentSettings");
    private static ClassName tableEnvName = ClassName.get("org.apache.flink.table.api.bridge.java", "StreamTableEnvironment");
    private static ClassName tumbleName = ClassName.get("org.apache.flink.table.api", "Tumble");

    private  static  final MethodSpec GenerateFilter(String name, String filter){
        return MethodSpec.methodBuilder(name)
                .addModifiers(Modifier.PRIVATE, Modifier.FINAL, Modifier.STATIC)
                .returns(tableName)
                .addParameter(tableEnvName, "tEnv")
                .addParameter(tableName, "source")
                .addStatement("$T table = $L.where($S)", tableName, "source", filter)
                .addStatement("$L.registerTable($S, $L)", "tEnv", name, "table")
                .addStatement("return table")
                .build();
    }

    private static final MethodSpec GenerateSelect(String name, List<String> select){
        return MethodSpec.methodBuilder(name)
                .addModifiers(Modifier.PRIVATE, Modifier.FINAL, Modifier.STATIC)
                .returns(tableName)
                .addParameter(tableEnvName, "tEnv")
                .addParameter(tableName, "source")
                .addStatement("$T table = $L.select($S)", tableName, "source", String.join(",", select))
                .addStatement("$L.registerTable($S, $L)", "tEnv", name, "table")
                .addStatement("return table")
                .build();
    }

    private  static  final MethodSpec GenerateTumble(String name, TumblingWindow window){
        return MethodSpec.methodBuilder(name)
                .addModifiers(Modifier.PRIVATE, Modifier.FINAL, Modifier.STATIC)
                .returns(tableName)
                .addParameter(tableEnvName, "tEnv")
                .addParameter(tableName, "source")
                .addStatement("$T table = $L.window($T.over($S).on($S).as($S))" +
                        ".groupBy($S).select($S)", tableName, "source", tumbleName, window.Over, window.On, window.As, window.GroupBy, String.join(",", window.Select))
                .addStatement("$L.registerTable($S, $L)", "tEnv", name, "table")
                .addStatement("return table")
                .build();
    }

    private  static  final MethodSpec GenerateSliding(String name, SlidingWindow window){
        return MethodSpec.methodBuilder(name)
                .addModifiers(Modifier.PRIVATE, Modifier.FINAL, Modifier.STATIC)
                .returns(tableName)
                .addParameter(tableEnvName, "tEnv")
                .addParameter(tableName, "source")
                .addStatement("$T table = $L.window(Tumble.over($S).every($S).on($S).as($S))" +
                        ".groupBy($S).select($S)", tableName, "source", window.Over,  window.Every, window.On, window.As, window.GroupBy, String.join(",", window.Select))
                .addStatement("$L.registerTable($S, $L)", "tEnv", name, "table")
                .addStatement("return table")
                .build();
    }

    private static String KafkaInitilization(Table table, KafkaConfiguration kafkaConfiguration){
        Configuration cfg = new Configuration(DEFAULT_INCOMPATIBLE_IMPROVEMENTS);

        try {
            cfg.setDirectoryForTemplateLoading(new File("C:\\Users\\HemanandRamasamy\\IdeaProjects\\QuickStart\\src\\main\\java\\com\\machdatum\\thingmanage\\templates")); //change absolute path
            Template template = cfg.getTemplate("KafkaSourceConfigurationTemplate.ftl");
            Map<String, Object> input = new HashMap<String, Object>();

            input.put("table",table);
            input.put("kafkaconfiguration",kafkaConfiguration);

            StringWriter out = new StringWriter();
            template.process(input, out);
            return out.toString();
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }

        return null;
    }

    public static String Generate(FlinkProcess process){
        List<MethodSpec> methods = new ArrayList<>();

        for (int i = 0; i < process.Transformations.size(); i++) {
            Object transformation = process.Transformations.get(i);

            if(transformation instanceof Filter){
                Filter filter = (Filter) transformation;
                MethodSpec method = GenerateFilter(filter.Name, filter.Condition);
                methods.add(method);
            }else if(transformation instanceof Select){
                Select select = (Select) transformation;
                MethodSpec method = GenerateSelect(select.Name, select.Fields);
                methods.add(method);
            }else if(transformation instanceof SlidingWindow){
                SlidingWindow window = (SlidingWindow) transformation;
                MethodSpec method = GenerateSliding(window.Name, window);
                methods.add(method);
            }else if(transformation instanceof TumblingWindow){
                TumblingWindow window = (TumblingWindow) transformation;
                MethodSpec method = GenerateTumble(window.Name, window);
                methods.add(method);
            }
        }

        String sourceInitilizer = KafkaInitilization(process.Schema, process.Source);

        MethodSpec Main = MethodSpec.methodBuilder("main")
                .addModifiers(Modifier.PUBLIC, Modifier.STATIC)
                .returns(void.class)
                .addParameter(String[].class, "args")
                .addStatement("$T env = $T.getExecutionEnvironment()", envName, envName)
                .addStatement("$T settings = $T.newInstance().inStreamingMode().useBlinkPlanner().build()", envSettingsName, envSettingsName)
                .addStatement("$T tEnv = $T.create(env,settings)", tableEnvName, tableEnvName)
                .addStatement(sourceInitilizer)
                .build();

        TypeSpec MainClass = TypeSpec.classBuilder("MainJob")
                .addModifiers(Modifier.PUBLIC, Modifier.FINAL)
                .addMethod(Main)
                .addMethods(methods)
                .build();

        JavaFile file = JavaFile.builder("com.machdatum.thingmanage", MainClass)
                .build();
        System.out.println(file.toString());

        return  file.toString();
    }
}
