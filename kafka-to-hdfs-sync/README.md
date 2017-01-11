### Description
The Kafka to HDFS Application Template continuously reads messages from configured Apache Kafka topic(s) and writes each message as a line in Hadoop HDFS file(s).
- The size of the files and rotation policy can be configured. So no small file problem in HDFS.
- The application scales linearly with the number of Kafka brokers and Kafka topics.
- The application is fault tolerant and can withstand node and cluster outages without data loss.
- The application is also highly performant and can process faster than Kafka broker can produce per topic.
- It is extremely easy to add custom logic to get your business value without worrying about connectivity and operational details of Kafka consumer and HDFS writer.
- The only configuration user needs to provide is source Kafka broker list, topic list and destination HDFS path, filename.
- This enterprise grade application template will dramatically reduce your time to market and cost of operations.

### Logical Plan

Here is a preview of the logical plan of the application template

![Logical Plan](https://www.datatorrent.com/wp-content/uploads/2016/11/Kafka_to_HDFS_DAG.png)

### Launch App Properties

Here is a preview of the properties to be set at application launch

![Launch App Properties](https://www.datatorrent.com/wp-content/uploads/2016/11/Kafka_to_HDFS_properties.png)

### Resources

Please find the walkthrough docs for app template as follows:

&nbsp; <a href="http://docs.datatorrent.com/app-templates/kafka-to-hdfs-sync"  class="docs" id="docs" ga-track="docs" target="_blank">http://docs.datatorrent.com/app-templates/kafka-to-hdfs-sync</a>

### Dynamic Partitioning of Deduper

This application enables the dedup operator to dynamically scale depending on the incoming load.
In this case it repartitions based on latency. There are parameters to adapt partitioning depending on latency:
1. latencyLowerBound - default = 1ms
2. latencyUpperBound - default = 10ms
If the latency of some operator goes above _latencyUpperBound_, then the number of partitions are increased to 2 (MAX_PARTITIONS)
If the latency of some operator goes below _latencyLowerBound_, then the number of partitions are decreased to 1 (MIN_PARTITIONS)

The cool off period of 30 secs is also provided so that there is not too much scale up and scale down too fast.

You can configure the above parameters when the app is running to get the desired partitioning effect. 
