package com.machdatum.thingmanage;

import com.squareup.javapoet.ClassName;
import freemarker.template.Configuration;
import freemarker.template.Template;
import org.apache.flink.streaming.connectors.kafka.config.StartupMode;
import org.apache.maven.shared.invoker.*;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;
import com.machdatum.thingmanage.model.*;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.Source;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import java.io.*;
import java.lang.reflect.Array;
import java.util.*;

public class App 
{
    private static ClassName tableName = ClassName.get("org.apache.flink.table.api", "Table");
    private static ClassName envName = ClassName.get("org.apache.flink.table.bridge.java", "StreamTableEnvironment");
    private static String Directory = "C:\\Users\\HemanandRamasamy\\Documents\\Generated";

    public static void main( String[] args ) {
        KafkaConfiguration source = new KafkaConfiguration(
                Arrays.asList("192.168.1.130:29092"),
                "testGroup",
                Arrays.asList("rawdata"),
                StartupMode.EARLIEST
        );

        List<Column> columns = Arrays.asList(
                new Column("Cnt", "INT", "Cnt"),
                new Column("Ts", "TIMESTAMP(3) METADATA FROM 'timestamp'", "Ts"),
                new Column("Device", "INT", "Device")
        );
        Table table = new Table("source", columns);
        List<Object> transformations = Arrays.asList(
            new TumblingWindow(
                    "Window1",
                    "1.minute",
                    "Ts",
                    "w",
                    Arrays.asList("w", "Device"),
                    Arrays.asList("Device", "w.start AS wStart", "w.end AS wEnd", "(MAX(Cnt) - MIN(Cnt)) AS Cnt" ))
        );

        FlinkProcess process = new FlinkProcess(source, table, transformations, null);

        SourceGenerator generator = new SourceGenerator();
        generator.Generate(process);

        try{
//            UpdatePOM();

            InvocationRequest request = new DefaultInvocationRequest();
            request.setGoals(Collections.singletonList("package"));
            Invoker invoker = new DefaultInvoker();
            invoker.setMavenHome(new File("C:\\Program Files\\Java\\apache-maven-3.6.3"));
            invoker.setWorkingDirectory(new File(Directory + "\\flink-process"));
            invoker.setOutputHandler(new InvocationOutputHandler() {
                @Override
                public void consumeLine(String s) throws IOException {
                    System.out.println(s);
                }
            });
            InvocationResult result = invoker.execute( request );
        }
        catch (Exception ex){

        }
    }

    private static Document AddDependency(Document document, String groupId, String artifactId, String version){
        NodeList list = document.getElementsByTagName("dependencies");
        Element dependencies = (Element) list.item(0);

        Element groupIdElement = document.createElement("groupId");
        groupIdElement.appendChild(document.createTextNode(groupId));
        Element artifactIdElement = document.createElement("artifactId");
        artifactIdElement.appendChild(document.createTextNode(artifactId));
        Element versionElement = document.createElement("version");
        versionElement.appendChild(document.createTextNode(version));

        Element dependency = document.createElement("dependency");

        dependency.appendChild(groupIdElement);
        dependency.appendChild(artifactIdElement);
        dependency.appendChild(versionElement);
        dependencies.appendChild(dependency);

        return  document;
    }

    private  static  void GenerateMaven() throws MavenInvocationException {
        InvocationRequest request = new DefaultInvocationRequest();
        request.setGoals(Collections.singletonList("archetype:generate"));

        Properties properties = new Properties();
        properties.setProperty("groupId", "com.machdatum.thingmanage");
        properties.setProperty("artifactId", "flink-process");
        properties.setProperty("archetypeVersion", "1.12.1");
        properties.setProperty("archetypeGroupId", "org.apache.flink");
        properties.setProperty("archetypeArtifactId", "flink-quickstart-java");
        properties.setProperty("interactiveMode", "false");

        request.setProperties(properties);

        Invoker invoker = new DefaultInvoker();
        invoker.setMavenHome(new File("C:\\Program Files\\Java\\apache-maven-3.6.3"));
        invoker.setWorkingDirectory(new File(Directory));
        invoker.setOutputHandler(new InvocationOutputHandler() {
            @Override
            public void consumeLine(String s) throws IOException {
                System.out.println(s);
            }
        });
        InvocationResult result = invoker.execute( request );

        if ( result.getExitCode() != 0 )
        {
            throw new IllegalStateException( "Build failed." );
        }
    }

    private static  void UpdatePOM() throws ParserConfigurationException, IOException, SAXException {
        File pom = new File(Directory + "\\flink-process\\pom.xml");
        DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
        DocumentBuilder builder = factory.newDocumentBuilder();
        Document document = builder.parse(pom);

        try{
            document.getDocumentElement().normalize();

            document = AddDependency(document, "org.apache.flink", "flink-connector-kafka_${scala.binary.version}", "${flink.version}");
            document = AddDependency(document, "org.apache.flink", "flink-table-api-java-bridge_${scala.binary.version}", "${flink.version}");

            NodeList list = document.getElementsByTagName("mainClass");
            Element mainClass = (Element) list.item(0);
            mainClass.setTextContent("com.machdatum.thingmanage.Main");

            TransformerFactory transformerFactory = TransformerFactory.newInstance();
            Transformer transformer = transformerFactory.newTransformer();
            transformer.setOutputProperty(OutputKeys.INDENT, "yes");
            DOMSource source = new DOMSource(document);
            StreamResult file = new StreamResult(new File(Directory + "\\flink-process\\pom.xml"));
            transformer.transform(source, file);
        }catch (Exception ex){
            System.out.println(ex);
        }

    }

}
