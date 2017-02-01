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

package com.datatorrent.apps.solution;

import com.datatorrent.api.DAG;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.annotation.ApplicationAnnotation;
import com.datatorrent.contrib.formatter.CsvFormatter;
import com.datatorrent.contrib.parser.CsvParser;
import org.apache.apex.malhar.kafka.KafkaSinglePortInputOperator;
import org.apache.apex.malhar.lib.fs.GenericFileOutputOperator.StringFileOutputOperator;
import org.apache.hadoop.conf.Configuration;

@ApplicationAnnotation(name = "Dedup-solution")
public class Application implements StreamingApplication
{

  public void populateDAG(DAG dag, Configuration conf)
  {
    KafkaSinglePortInputOperator kafkaInputOperator = dag.addOperator("kafkaInput", KafkaSinglePortInputOperator.class);

    CsvParser parser = dag.addOperator("csvParser", CsvParser.class);
    Deduper deduper = dag.addOperator("deduper", Deduper.class);
    CsvFormatter formatter = dag.addOperator("csvFormatter", CsvFormatter.class);

    StringFileOutputOperator fileOutput = dag.addOperator("fileOutput", StringFileOutputOperator.class);

    dag.addStream("toParser", kafkaInputOperator.outputPort, parser.in);
    dag.addStream("toDedup", parser.out, deduper.input);
    dag.addStream("toFormatter", deduper.unique, formatter.in);
    dag.addStream("toHDFS", formatter.out, fileOutput.input);
  }

}
