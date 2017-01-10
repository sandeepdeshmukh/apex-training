/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.datatorrent.apps;

import java.util.Map;

import org.apache.apex.malhar.kafka.KafkaSinglePortInputOperator;
import org.apache.apex.malhar.lib.fs.GenericFileOutputOperator.BytesFileOutputOperator;
import org.apache.apex.malhar.lib.fs.GenericFileOutputOperator.StringFileOutputOperator;
import org.apache.hadoop.conf.Configuration;

import com.google.common.collect.Maps;

import com.datatorrent.api.DAG;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.annotation.ApplicationAnnotation;
import com.datatorrent.contrib.formatter.CsvFormatter;
import com.datatorrent.contrib.parser.CsvParser;
import com.datatorrent.lib.transform.TransformOperator;

@ApplicationAnnotation(name = "Kafka-to-HDFS-Sync")
public class Application implements StreamingApplication
{

  public void populateDAG(DAG dag, Configuration conf)
  {
    KafkaSinglePortInputOperator kafkaInputOperator = dag.addOperator("kafkaInput", KafkaSinglePortInputOperator.class);
    BytesFileOutputOperator fileOutput = dag.addOperator("fileOutput", BytesFileOutputOperator.class);

    dag.addStream("data", kafkaInputOperator.outputPort, fileOutput.input);

    /*
     * To add custom logic to your DAG, add your custom operator here with
     * dag.addOperator api call and connect it in the dag using the dag.addStream
     * api call. 
     * 
     * For example: 
     * 
     * To parse incoming csv lines, transform them and outputting them on kafka.
     * kafkaInput->CSVParser->Transform->CSVFormatter->fileOutput can be achieved as follows
     * 
     * Adding operators: 
     * CsvParser csvParser = dag.addOperator("csvParser", CsvParser.class);
     *
     * TransformOperator transform = dag.addOperator("transform", new TransformOperator());
     * Map<String, String> expMap = Maps.newHashMap();
     * expMap.put("name", "{$.name}.toUpperCase()");
     * transform.setExpressionMap(expMap);
     * CsvFormatter formatter = dag.addOperator("formatter", new CsvFormatter());
     * 
     * Use StringFileOutputOperator instead of BytesFileOutputOperator i.e. 
     * Replace the following line below:
     * BytesFileOutputOperator fileOutput = dag.addOperator("fileOutput", BytesFileOutputOperator.class);
     * with this lines:
     * StringFileOutputOperator fileOutput = dag.addOperator("fileOutput", new StringFileOutputOperator());
     *
     * Connect these operators with approriate streams
     * Replace the following line below:
     * dag.addStream("data", kafkaInputOperator.outputPort, fileOutput.input);
     * 
     * with these lines:
     * dag.addStream("data", kafkaInputOperator.outputPort, csvParser.in);
     * dag.addStream("pojo", csvParser.out, transform.input);
     * dag.addStream("transformed", transform.output, formatter.in);
     * dag.addStream("string", formatter.out, fileOutput.input);
     * 
     * In ApplicationTests.java->
     * Replace the following line from setup()
     * outputFilePath = outputDir + "/output.txt_2.0";
     * with
     * outputFilePath = outputDir + "/output.txt_5.0";
     * 
     * Replace the following line from compare()
     * Assert.assertArrayEquals(lines, output.split("\\n"));
     * with
     * Assert.assertArrayEquals(lines_transformed, output.split("\\n"));
     * 
     */
  }

}
