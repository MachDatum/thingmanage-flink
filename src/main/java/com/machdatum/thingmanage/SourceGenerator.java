package com.machdatum.thingmanage;

import com.machdatum.thingmanage.model.*;
import com.squareup.javapoet.ClassName;
import com.squareup.javapoet.JavaFile;
import com.squareup.javapoet.MethodSpec;
import com.squareup.javapoet.TypeSpec;
import freemarker.template.Configuration;
import freemarker.template.Template;
import org.apache.flink.table.api.TableColumn;
import org.apache.flink.table.api.TableSchema;

import javax.lang.model.element.Modifier;
import java.io.File;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static freemarker.template.Configuration.DEFAULT_INCOMPATIBLE_IMPROVEMENTS;

public class SourceGenerator {
    private static ClassName tableName = ClassName.get("org.apache.flink.table.api", "Table");
    private static ClassName envName = ClassName.get("org.apache.flink.streaming.api.environment", "StreamExecutionEnvironment");
    private static ClassName envSettingsName = ClassName.get("org.apache.flink.table.api", "EnvironmentSettings");
    private static ClassName tableColumn = ClassName.get("org.apache.flink.table.api", "TableColumn");
    private static ClassName tableEnvName = ClassName.get("org.apache.flink.table.api.bridge.java", "StreamTableEnvironment");
    private static ClassName tumbleName = ClassName.get("org.apache.flink.table.api", "Tumble");
    private static ClassName listName = ClassName.get("java.util", "List");

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
//        String identifiers = window.Select == null ? GenerateFields(window.Fields) :  String.join(",", window.Select);
        return MethodSpec.methodBuilder(name)
                .addModifiers(Modifier.PRIVATE, Modifier.FINAL, Modifier.STATIC)
                .returns(tableName)
                .addParameter(tableEnvName, "tEnv")
                .addParameter(tableName, "source")
//                .addStatement("$T table = $L.window($T.over(lit($S).minutes()).on($S).as($S)" +
                .addStatement("$T table = $L.window($T.over($S).on($S).as($S))" +
                        ".groupBy($S)" +
                        ".select($S)",
                        tableName, "source", tumbleName, window.Over,window.On,
                        window.As, String.join(",", window.GroupBy), String.join(",", window.Select))
                .addStatement("$L.registerTable($S, $L)", "tEnv", name, "table")
                .addStatement("return table")
                .build();
    }

    public static String GenerateFields(List<Field> fields){
        StringBuilder out = new StringBuilder("");
        for(Field field : fields){
            out.append("$(\""+ field.Source + "\")");
            if(field.Func != null){
                out.append("."+ field.Func + "");
            }
            if(field.Func != null){
                out.append(".as(\""+ field.As+"\")");
            }
            out.append(",");
        }
        String out_ = out.toString();
        return out_.substring(0, out_.length() -1);
    }

    public static String GenerateGroupBy(List<String> fields){
        StringBuilder out = new StringBuilder("");
        for(String field : fields){
            out.append("$(\""+ field + "\"),");
        }
        String out_ = out.toString();
        return out_.substring(0, out_.length() -1);
    }

    private  static  final MethodSpec GenerateSliding(String name, SlidingWindow window){
        return MethodSpec.methodBuilder(name)
                .addModifiers(Modifier.PRIVATE, Modifier.FINAL, Modifier.STATIC)
                .returns(tableName)
                .addParameter(tableEnvName, "tEnv")
                .addParameter(tableName, "source")
                .addStatement("$T table = $L.window(Tumble.over($S).every($S).on($S).as($S))" +
                        ".groupBy($S).select($S)", tableName, "source", window.Over,  window.Every, window.On, window.As, window.GroupBy, GenerateFields(window.Fields))
                .addStatement("$L.registerTable($S, $L)", "tEnv", name, "table")
                .addStatement("return table")
                .build();
    }

    private static String KafkaInitilization(Table table, KafkaConfiguration kafka){
        Configuration cfg = new Configuration(DEFAULT_INCOMPATIBLE_IMPROVEMENTS);

        try {
            cfg.setDirectoryForTemplateLoading(new File("/home/aurora/Documents/thingmanage-flink/src/main/java/com/machdatum/thingmanage/templates")); //change absolute path
            Template template = cfg.getTemplate("KafkaSourceConfigurationTemplate.ftl");
            Map<String, Object> input = new HashMap<String, Object>();

            input.put("table",table);
            input.put("kafkaconfiguration", kafka);

            StringWriter out = new StringWriter();
            template.process(input, out);
            return out.toString();
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }

        return null;
    }

    public static String GetSchema(TableSchema schema){
        StringBuilder fields = new StringBuilder("");
        List<TableColumn> columns = schema.getTableColumns();
        for (TableColumn column : columns){
            fields.append(column.getName()+ " ");
            fields.append(column.getType());
            fields.append(",");
        }

        String out = fields.toString().replace("*ROWTIME*", "");;
        return out.substring(0, out.length() -1);
    }

    public static String ElasticSink(String TableName){
        Configuration cfg = new Configuration(DEFAULT_INCOMPATIBLE_IMPROVEMENTS);

        try {
            cfg.setDirectoryForTemplateLoading(new File("/home/aurora/Documents/thingmanage-flink/src/main/java/com/machdatum/thingmanage/templates")); //change absolute path
            Template template = cfg.getTemplate("ElasticSinkConfigurationTemplate.ftl");
            Map<String, Object> input = new HashMap<String, Object>();

            input.put("tablename", TableName);

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
        StringBuilder funcCaller = new StringBuilder("Table source = tEnv.from(\"source\");");
        funcCaller.append("\n");
        String Table_ = "source";
        for (int i = 0; i < process.Transformations.size(); i++) {
            Object transformation = process.Transformations.get(i);
            if(transformation instanceof Filter){
                Filter filter = (Filter) transformation;
                MethodSpec method = GenerateFilter(filter.Name, filter.Condition);
                methods.add(method);
                funcCaller.append("Table table"+i+" = " + filter.Name+"(tEnv," + Table_+");");
                funcCaller.append("\n");
                Table_ = "table"+i;
            }else if(transformation instanceof Select){
                Select select = (Select) transformation;
                MethodSpec method = GenerateSelect(select.Name, select.Fields);
                methods.add(method);
                funcCaller.append("Table table"+i+" = " + select.Name+"(tEnv," + Table_+");");
                funcCaller.append("\n");
                Table_ = "table"+i;
            }else if(transformation instanceof SlidingWindow){
                SlidingWindow window = (SlidingWindow) transformation;
                MethodSpec method = GenerateSliding(window.Name, window);
                methods.add(method);
                funcCaller.append("Table table"+i+" = " + window.Name+"(tEnv," + Table_+");");
                funcCaller.append("\n");
                Table_ = "table"+i;
            }else if(transformation instanceof TumblingWindow){
                TumblingWindow window = (TumblingWindow) transformation;
                MethodSpec method = GenerateTumble(window.Name, window);
                methods.add(method);
                funcCaller.append("Table table"+i+" = " + window.Name+"(tEnv," + Table_+");");
                funcCaller.append("\n");
                Table_ = "table"+i;
            }
        }

        String sourceInitilizer = KafkaInitilization(process.Schema, process.Source);
        String sink = ElasticSink(Table_);
        funcCaller.append(sink);

        MethodSpec GenerateSchema = MethodSpec.methodBuilder("GenerateSchema")
                .addModifiers(Modifier.PUBLIC,Modifier.STATIC)
                .returns(String.class)
                .addParameter(TableSchema.class, "schema")
                .addStatement("StringBuilder fields = new StringBuilder(\"\")")
                .addStatement("$T<TableColumn> columns = schema.getTableColumns()",listName)
                .addCode("for ($T column : columns){", tableColumn)
                .addCode("\n")
                .addStatement("fields.append(column.getName()+ \" \")")
                .addStatement("fields.append(column.getType())")
                .addStatement("fields.append(\",\")")
                .addCode("}")
                .addCode("\n")
                .addStatement("String out =fields.toString().replace(\"*ROWTIME*\", \"\")")
                .addStatement("return out.substring(0, out.length() -1)")
                .build();

        MethodSpec Main = MethodSpec.methodBuilder("main")
                .addModifiers(Modifier.PUBLIC, Modifier.STATIC)
                .returns(void.class)
                .addParameter(String[].class, "args")
                .addStatement("$T env = $T.getExecutionEnvironment()", envName, envName)
                .addStatement("$T settings = $T.newInstance().inStreamingMode().useBlinkPlanner().build()", envSettingsName, envSettingsName)
                .addStatement("$T tEnv = $T.create(env,settings)", tableEnvName, tableEnvName)
                .addStatement(sourceInitilizer)
                .addStatement(funcCaller.toString())
                .build();

        TypeSpec MainClass = TypeSpec.classBuilder("MainJob")
                .addModifiers(Modifier.PUBLIC, Modifier.FINAL)
                .addMethod(GenerateSchema)
                .addMethod(Main)
                .addMethods(methods)
                .build();

        JavaFile file = JavaFile.builder("com.machdatum.thingmanage", MainClass)
                .build();
        System.out.println(file.toString());

        return  file.toString();
    }
}
