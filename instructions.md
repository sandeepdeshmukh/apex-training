This is step-by-step guide for the Apex Trainer to conduct hands on sessions. A good document to refer would be [Beginners Guide](http://docs.datatorrent.com/beginner/)

# Project creation using archetype

### Create project
```shell
v="3.5.0"
mvn -B archetype:generate \
  -DarchetypeGroupId=org.apache.apex \
  -DarchetypeArtifactId=apex-app-archetype \
  -DarchetypeVersion="$v" \
  -DgroupId=com.example \
  -Dpackage=com.example.myapexapp \
  -DartifactId=myapexapp \
  -Dversion=1.0-SNAPSHOT
```

### Compile and verify 
```bash
mvn clean package -DskipTests
```

### Folder structure walkthrough
1. Walk through the folders and code
..1. src, test, resources/META-INF/properties.xml, pom.xml - dependencies

### Run and show the DAG 
```bash
apex
launch target/myapexapp-1.0-SNAPSHOT.apa
```

### Go through the DTConsole
1. Run through the monitor console
  * Main monitor page 
  * Cluster Overview
  * DataTorrent Applications

2. Monitor an application
  * Logical
  * Physical
  * Physical DAG
  * Metrics
  * Attempts

### Change few parameters/properties to make it unique during the training

1. Change in Application.java @ApplicationAnnotation to dedup-your-name
1. Change in pom.xml artifactid to dedup-your-name

Compile and run again.

### Setup cluster environment for running the application
1. Setup SSH key & config to connect to the cluster
1. Login to remote cluster and create a personal folder
1. rsync using ssh your code within that folder 
  * rsync -avz -e "ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null"  ../myapexapp  training@X.X.X.X:/home/training/code/your-name (exact command in the workbook)
  * Exclude target folder
1. Compile and run the application on remote cluster
1. Access dtConsole and verify that the app is running successfully

# Writing Kafka to HDFS Sync App

### Add kafka dependency in pom.xml
```bash
    <dependency>
      <groupId>org.apache.kafka</groupId>
      <artifactId>kafka_2.11</artifactId>
      <version>0.9.0.1</version>
    </dependency>
        <dependency>
      <groupId>org.apache.apex</groupId>
      <artifactId>malhar-kafka</artifactId>
      <version>${malhar.version}</version>
      <exclusions>
        <exclusion>
          <groupId>*</groupId>
          <artifactId>*</artifactId>
        </exclusion>
      </exclusions>
    </dependency>
```

### Add malhar.version property 
```diff
 <properties>
    <!-- change this if you desire to use a different version of Apex Core -->
    <apex.version>3.5.0</apex.version>
+    <malhar.version>3.7.0</malhar.version>
    <apex.apppackage.classpath>lib/*.jar</apex.apppackage.classpath>
  </properties>
```

### Add following into the properties.xml file. 
```diff
<configuration>
  <property>
    <name>dt.operator.kafkaInput.prop.clusters</name>
    <value>localhost:9092</value>
  </property> 
  <property>
    <name>dt.operator.kafkaInput.prop.topics</name>
    <value>transactions</value>
  </property>
  <property>
    <name>dt.operator.kafkaInput.prop.initialOffset</name>
    <value>EARLIEST</value>
  </property>
  <property>
    <name>dt.operator.fileOutput.prop.filePath</name>
    <value>/tmp/<your-name></value>
  </property>
  <property>
    <name>dt.operator.fileOutput.prop.outputFileName</name>
    <value>output.txt</value>
  </property>
</configuration>
```

## Setup kafka 
For details: [Kafka QuickStart](https://kafka.apache.org/quickstart)

### Send some messages
```bash
cat ~/kaf*/sampleData.txt | kafka-console-producer.sh --broker-list localhost:9093 --topic transactions
```

## Change Application.java 

```diff
+	// Add operators
+	KafkaSinglePortInputOperator kafkaInput = dag.addOperator("kafkaInput", KafkaSinglePortInputOperator.class);
+	BytesFileOutputOperator hdfsOutput = dag.addOperator("fileOutput", BytesFileOutputOperator.class);

	// Add stream
+	dag.addStream("Kafka2HDFS", kafkaInput.outputPort, hdfsOutput.input);
```
Remove all other/older lines from the function.

### Check for data in the output folder
```bash
hdfs dfs -ls /tmp/<your-name>
```

# Writing a new operator
Write a new operator called Dedup in following steps.

## Create a new class
Create a new class - File -> New -> Class ,
Name: Dedup,
SuperClass -> BaseOperator

### Pass through operator
```diff
    public final transient DefaultInputPort<Object> input = new DefaultInputPort<Object>() {
        @Override
        public void process(Object tuple) {
	    output.emit((byte[])tuple)
	}
    };

    public final transient DefaultOutputPort<byte[]> output = new DefaultOutputPort<byte[]>();
```

### Update Application.java
```diff
  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
    // Add operators
    KafkaSinglePortInputOperator kafkaInput = dag.addOperator("kafkaInput", KafkaSinglePortInputOperator.class);
+   Dedup dedup = dag.addOperator("dedup", Dedup.class);
    BytesFileOutputOperator hdfsOutput = dag.addOperator("fileOutput", BytesFileOutputOperator.class);
+   ConsoleOutputOperator console = dag.addOperator("console", ConsoleOutputOperator.class);

    // Add stream
-   dag.addStream("Kafka2HDFS", kafkaInput.outputPort, hdfsOutput.input);
+   dag.addStream("Kafka2Dedup", kafkaInput.outputPort, dedup.input);
+   dag.addStream("Dedup2HDFS", dedup.unique, hdfsOutput.input);
+   dag.addStream("Duplicate2Console", dedup.duplicate, console.input);
  }

```



