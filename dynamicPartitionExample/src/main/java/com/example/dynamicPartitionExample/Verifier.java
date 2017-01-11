package com.example.dynamicPartitionExample;

import java.util.HashSet;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.common.util.BaseOperator;

/**
 * Created by bhupesh on 11/1/17.
 */
public class Verifier extends BaseOperator
{
  private HashSet<Integer> state = new HashSet<>();
  private HashSet<Integer> windowDuplicates = new HashSet<>();

  private static final Logger LOG = LoggerFactory.getLogger(Verifier.class);

  public final transient DefaultInputPort<Integer> unique = new DefaultInputPort<Integer>()
  {
    @Override
    public void process(Integer tuple)
    {
      if (state.contains(tuple)) {
        LOG.error("Unique tuple received again {}", tuple);
      } else {
        state.add(tuple);
        LOG.info("UNIQUE OKAY");
      }
    }
  };

  public final transient DefaultInputPort<Integer> duplicate = new DefaultInputPort<Integer>()
  {
    @Override
    public void process(Integer tuple)
    {
      windowDuplicates.add(tuple);
    }
  };

  @Override
  public void endWindow()
  {
    for (int i: windowDuplicates) {
      if (!state.contains(i)) {
        LOG.error("Not a duplicate yet {}", i);
      } else {
        state.add(i);
        LOG.info("DUPLICATE OKAY");
      }
    }
    windowDuplicates.clear();
  }
}
