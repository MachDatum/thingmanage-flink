package com.machdatum.thingmanage;

import com.squareup.javapoet.ClassName;
import org.apache.flink.streaming.connectors.kafka.config.StartupMode;
import org.apache.maven.shared.invoker.*;
import org.w3c.dom.Attr;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;
import com.machdatum.thingmanage.model.*;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import java.io.File;
import java.io.*;
import java.util.*;

public class App 
{
    private static ClassName tableName = ClassName.get("org.apache.flink.table.api", "Table");
    private static ClassName envName = ClassName.get("org.apache.flink.table.bridge.java", "StreamTableEnvironment");
    private static String Directory = "/home/aurora/Documents/generated";

    public static void main( String[] args ) throws MavenInvocationException, ParserConfigurationException, SAXException, IOException {
        KafkaConfiguration source = new KafkaConfiguration(
                Arrays.asList("192.168.1.130:29092"),
                "testGroup",
                Arrays.asList("rawdata"),
                StartupMode.SPECIFIC_OFFSETS
        );

        List<Column> columns = Arrays.asList(
                new Column("Cnt", "INT", "Cnt"),
//                new Column("Ts", "TIMESTAMP(3) METADATA FROM 'timestamp'", "Ts"),
                new Column("Device", "INT", "Device"),
                new Watermark("Ts", "TIMESTAMP(3) METADATA FROM 'timestamp'", "Ts", 5, TimeUnit.SECOND)

        );
        Table table = new Table("source", columns);
        List<Object> transformations = Arrays.asList(
            new TumblingWindow(
                    "Window1",
                    "1.minute",
                    "Ts",
                    "w",
                    Arrays.asList("w", "Device"),
//                    Arrays.asList(new Field("Device",null,null),new Field("w","wStart","start"),new Field("w","wEnd","end()")))
                    Arrays.asList("Device", "w.start AS wStart", "w.end AS wEnd", "(MAX(Cnt) - MIN(Cnt)) AS Cnt" ))
        );

        FlinkProcess process = new FlinkProcess(source, table, transformations, null);

        SourceGenerator generator = new SourceGenerator();
        String sourceCode = generator.Generate(process);

        try{
            GenerateMaven();
            UpdatePOM();

            File tempDirectory = new File(Directory + "/flink-process/src/main/java/com/machdatum/thingmanage/");
            File fileWithAbsolutePath = new File(tempDirectory, "MainJob.java");
            if(!fileWithAbsolutePath.exists()){
                fileWithAbsolutePath.createNewFile();
            }
            FileWriter  file = new FileWriter (fileWithAbsolutePath);
            file.write(sourceCode);
            file.close();

            InvocationRequest request = new DefaultInvocationRequest();
            request.setGoals(Collections.singletonList("package"));
            Invoker invoker = new DefaultInvoker();
            invoker.setMavenHome(new File("/home/aurora/.m2/wrapper/dists/apache-maven-3.6.0-bin/2dakv70gp803gtm5ve1ufmvttn/apache-maven-3.6.0/"));
            invoker.setWorkingDirectory(new File(Directory + "/flink-process"));
            invoker.setOutputHandler(new InvocationOutputHandler() {
                @Override
                public void consumeLine(String s) throws IOException {
                    System.out.println(s);
                }
            });
            InvocationResult result = invoker.execute( request );
        }
        catch (Exception ex){
            throw ex;
        }
    }

    private static Document AddTransformer(Document document, String implementation){
        NodeList list = document.getElementsByTagName("transformers");
        Element transformers = (Element) list.item(0);

        Element transformer = document.createElement("transformer");
        Attr attr = document.createAttribute("implementation");
        attr.setNodeValue(implementation);
        transformer.setAttributeNode(attr);

        transformers.appendChild(transformer);
        return  document;
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
        invoker.setMavenHome(new File("/home/aurora/.m2/wrapper/dists/apache-maven-3.6.0-bin/2dakv70gp803gtm5ve1ufmvttn/apache-maven-3.6.0/"));
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
        File pom = new File(Directory + "/flink-process/pom.xml");
        DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
        DocumentBuilder builder = factory.newDocumentBuilder();
        Document document = builder.parse(pom);

        try{
            document.getDocumentElement().normalize();

            document = AddDependency(document, "org.apache.flink", "flink-connector-kafka_${scala.binary.version}", "${flink.version}");
            document = AddDependency(document, "org.apache.flink", "flink-table-api-java-bridge_${scala.binary.version}", "${flink.version}");
            document = AddDependency(document, "org.apache.flink", "flink-json","${flink.version}");
            document = AddDependency(document, "org.apache.flink", "flink-connector-elasticsearch7_${scala.binary.version}","${flink.version}");
            document = AddTransformer(document, "org.apache.maven.plugins.shade.resource.ServicesResourceTransformer");

            NodeList list = document.getElementsByTagName("mainClass");
            Element mainClass = (Element) list.item(0);
            mainClass.setTextContent("com.machdatum.thingmanage.MainJob");

            TransformerFactory transformerFactory = TransformerFactory.newInstance();
            Transformer transformer = transformerFactory.newTransformer();
            transformer.setOutputProperty(OutputKeys.INDENT, "yes");
            DOMSource source = new DOMSource(document);
            StreamResult file = new StreamResult(new File(Directory + "/flink-process/pom.xml"));
            transformer.transform(source, file);
        }catch (Exception ex){
            System.out.println(ex);
        }

    }
}
