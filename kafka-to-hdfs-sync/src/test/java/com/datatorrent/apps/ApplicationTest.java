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

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;

import javax.validation.ConstraintViolationException;

import org.apache.apex.malhar.kafka.AbstractKafkaInputOperator;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import info.batey.kafka.unit.KafkaUnitRule;
import info.batey.kafka.unit.KafkaUnit;

import kafka.producer.KeyedMessage;

import com.datatorrent.api.LocalMode;

import static org.junit.Assert.assertTrue;

/**
 * Test the DAG declaration in local mode.
 */
public class ApplicationTest
{
  private static final Logger LOG = LoggerFactory.getLogger(ApplicationTest.class);
  private static final int zkPort = 12181;
  private static final int brokerPort = 19092;
  private String outputDir;
  private String outputFilePath;
  private String topic;

  public static class TestMeta extends TestWatcher
  {
    public String baseDirectory;

    @Override
    protected void starting(org.junit.runner.Description description)
    {
      this.baseDirectory = "target/" + description.getClassName() + "/" + description.getMethodName();
    }

    @Override
    protected void finished(Description description)
    {
      super.finished(description);
      try {
        FileUtils.forceDelete(new File(baseDirectory));
      } catch (IOException e) {
        e.printStackTrace();
      }
    }

  }

  @Rule
  public TestMeta testMeta = new TestMeta();

  @Before
  public void setup() throws Exception
  {
    outputDir = testMeta.baseDirectory + File.separator + "output";
    outputFilePath = outputDir + "/output.txt_2.0";
  }

  // test messages
  private static String[] lines = { "1|User_1|1000", "2|User_2|2000", "3|User_3|3000", "4|User_4|4000", "5|User_5|5000",
      "6|User_6|6000", "7|User_7|7000", "8|User_8|8000", "9|User_9|9000", "10|User_10|10000" };

  //test messages
  private static String[] lines_transformed = { "1|USER_1|1000", "2|USER_2|2000", "3|USER_3|3000", "4|USER_4|4000", "5|USER_5|5000",
      "6|USER_6|6000", "7|USER_7|7000", "8|USER_8|8000", "9|USER_9|9000", "10|USER_10|10000" };

  // broker port must match properties.xml
  @Rule
  public KafkaUnitRule kafkaUnitRule = new KafkaUnitRule(zkPort, brokerPort);

  @Test
  public void testApplication() throws Exception
  {
    try {

      // write messages to Kafka topic
      Configuration conf = getConfig();

      writeToTopic();

      // run app asynchronously; terminate after results are checked
      LocalMode.Controller lc = asyncRun(conf);

      // check for presence of output file
      chkOutput();

      // compare output lines to input
      compare();

      lc.shutdown();
    } catch (ConstraintViolationException e) {
      Assert.fail("constraint violations: " + e.getConstraintViolations());
    }
  }

  private void writeToTopic()
  {
    KafkaUnit ku = kafkaUnitRule.getKafkaUnit();
    ku.createTopic(topic);
    for (String line : lines) {
      KeyedMessage<String, String> kMsg = new KeyedMessage<>(topic, line);
      ku.sendMessages(kMsg);
    }
    LOG.debug("Sent messages to topic {}", topic);
  }

  private Configuration getConfig()
  {
    Configuration conf = new Configuration(false);
    conf.addResource(this.getClass().getResourceAsStream("/META-INF/properties-test.xml"));
    conf.set("dt.operator.fileOutput.prop.filePath", outputDir);
    topic = conf.get("dt.operator.kafkaInput.prop.topics");

    return conf;
  }

  private LocalMode.Controller asyncRun(Configuration conf) throws Exception
  {

    LocalMode lma = LocalMode.newInstance();
    lma.prepareDAG(new Application(), conf);
    LocalMode.Controller lc = lma.getController();
    lc.runAsync();
    return lc;
  }

  private void chkOutput() throws Exception
  {
    File file = new File(outputFilePath);
    final int MAX = 60;
    for (int i = 0; i < MAX && (!file.exists()); ++i) {
      LOG.debug("Sleeping, i = {}", i);
      Thread.sleep(1000);
    }
    if (!file.exists()) {
      String msg = String.format("Error: %s not found after %d seconds%n", outputFilePath, MAX);
      throw new RuntimeException(msg);
    }
  }

  private void compare() throws Exception
  {
    // read output file
    File file = new File(outputFilePath);
    String output = FileUtils.readFileToString(file);
    Assert.assertArrayEquals(lines, output.split("\\n"));
  }
}
