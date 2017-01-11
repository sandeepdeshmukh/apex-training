/**
 * Put your copyright and license info here.
 */
package com.example.dynamicPartitionExample;

import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.DAG;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.annotation.ApplicationAnnotation;

@ApplicationAnnotation(name="DynamicDedup")
public class Application implements StreamingApplication
{

  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
    RandomNumberGenerator randomGenerator = dag.addOperator("randomGenerator", RandomNumberGenerator.class);

    Dedup dedup = dag.addOperator("dedup", Dedup.class);

//    ConsoleOutputOperator consUnique = dag.addOperator("consoleUnique", new ConsoleOutputOperator());
//    consUnique.setSilent(true);
//    ConsoleOutputOperator consDuplicate = dag.addOperator("consoleDuplicate", new ConsoleOutputOperator());
//    consDuplicate.setSilent(true);

    Verifier verifier = dag.addOperator("verifier", new Verifier());

    dag.addStream("randomData", randomGenerator.out, dedup.input).setLocality(DAG.Locality.CONTAINER_LOCAL);
    dag.addStream("uniqueData", dedup.unique, verifier.unique);
    dag.addStream("duplicateData", dedup.duplicate, verifier.duplicate);
  }
}
