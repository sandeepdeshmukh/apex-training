package com.example.dynamicPartitionExample;

import java.io.ByteArrayOutputStream;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.commons.io.IOUtils;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import com.datatorrent.api.Context;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.DefaultPartition;
import com.datatorrent.api.Partitioner;
import com.datatorrent.api.StatsListener;
import com.datatorrent.common.util.BaseOperator;

public class Dedup extends BaseOperator implements Partitioner<Dedup>, StatsListener
{
  private static final Logger LOG = LoggerFactory.getLogger(Dedup.class);

  private static final int MAX_PARTITIONS = 2;
  private static final int MIN_PARTITIONS = 1;
  private static final long COOL_OFF_MILLIS = 30*1000;
  private HashSet<Integer> data = new HashSet<>();
  private int numPartitions = 1;
  private long lastRepartitionTime = System.currentTimeMillis();
  private Set<Integer> partitionKeys;
  private int partitionMask;
  private int latencyLowerBound = 1;
  private int latencyUpperBound = 10;


  @Override
  public void setup(Context.OperatorContext context)
  {
    partitionKeys = Sets.newHashSet(0);
    partitionMask = 0;
  }

  public final transient DefaultInputPort<Integer> input = new DefaultInputPort<Integer>()
  {
    @Override
    public void process(Integer tuple)
    {
      if (isDuplicate(tuple)) {
        duplicate.emit(tuple);
      } else {
        data.add(tuple);
        unique.emit(tuple);
      }
    }
  };

  public final transient DefaultOutputPort<Integer> unique = new DefaultOutputPort<>();
  public final transient DefaultOutputPort<Integer> duplicate = new DefaultOutputPort<>();

  public boolean isDuplicate(Integer tuple)
  {
    if (data.contains(tuple)) {
      return true;
    }
    return false;
  }

  @Override
  public Collection<Partition<Dedup>> definePartitions(Collection<Partition<Dedup>> partitions, PartitioningContext context)
  {
    final int newPartitionCount = DefaultPartition.getRequiredPartitionCount(context, this.numPartitions);

    LOG.info("Define Partitions");
    if(newPartitionCount <= 0) {
      throw new RuntimeException("Number of partitions should be >= 1");
    }

    int oldPartitions = partitions.size();

    if (oldPartitions == newPartitionCount) {
      LOG.info("definePartitions: Nothing to do in definePartitions");
      return partitions;
    }
    LOG.debug("definePartitions: Repartitioning from {} to {}", oldPartitions, newPartitionCount);

    // Collect old state
    HashSet<Integer> state = Sets.newHashSet();
    for (Partition<Dedup> partition: partitions) {
      state.addAll(partition.getPartitionedInstance().data);
    }

    LOG.info("Old State: " + state);
    Kryo kryo = new Kryo();
    List<Partition<Dedup>> newPartitions = Lists.newArrayListWithExpectedSize(newPartitionCount);

    for (int i = 0; i < newPartitionCount; i++) {
      Dedup operator = cloneObject(kryo, this);
      newPartitions.add(new DefaultPartition<>(operator));
    }

    // Assign partition keys
    List<InputPort<?>> inputPortList = context.getInputPorts();
    InputPort<?> inputPort = inputPortList.iterator().next();
    DefaultPartition.assignPartitionKeys(Collections.unmodifiableCollection(newPartitions), inputPort);
    int commonMask = newPartitions.iterator().next().getPartitionKeys().get(inputPort).mask;

    // distribute state to new partitions
    for (Partition<Dedup> dedupPartition: newPartitions) {
      Dedup dedupInstance = dedupPartition.getPartitionedInstance();
      dedupInstance.partitionKeys = dedupPartition.getPartitionKeys().get(inputPort).partitions;
      dedupInstance.partitionMask = commonMask;

      for (int key: state) {
        int partitionKey = Integer.valueOf(key).hashCode() & commonMask;
        if (dedupInstance.partitionKeys.contains(partitionKey)) {
          dedupInstance.data.add(key);
        }
      }
      LOG.info("State for partition: " + dedupInstance.data);
    }
    return newPartitions;
  }

  @Override
  public void partitioned(Map<Integer, Partition<Dedup>> partitions)
  {
    if (numPartitions != partitions.size()) {
      String msg = String.format("partitions = %d, map.size = %d%n", partitions, partitions.size());
      throw new RuntimeException(msg);
    }
  }

  @Override
  public Response processStats(BatchedOperatorStats stats)
  {
    final long latency = stats.getLatencyMA();
//    final long totalEmitted = stats.getTuplesEmittedPSMA();
    Response response = new Response();
    response.repartitionRequired = false;

    if (System.currentTimeMillis() - lastRepartitionTime > COOL_OFF_MILLIS) { // cool off
      LOG.info("Latency: " + latency);
      if(latency > latencyUpperBound && numPartitions < MAX_PARTITIONS) {
        numPartitions = MAX_PARTITIONS;
        lastRepartitionTime = System.currentTimeMillis();
        response.repartitionRequired = true;
      } else if (latency < latencyLowerBound && numPartitions == MAX_PARTITIONS) {
        numPartitions = MIN_PARTITIONS;
        lastRepartitionTime = System.currentTimeMillis();
        response.repartitionRequired = true;
      }
    }
    return response;
  }

  @SuppressWarnings("unchecked")
  private static <SRC> SRC cloneObject(Kryo kryo, SRC src)
  {
    kryo.setClassLoader(src.getClass().getClassLoader());
    ByteArrayOutputStream bos = null;
    Output output;
    Input input = null;
    try {
      bos = new ByteArrayOutputStream();
      output = new Output(bos);
      kryo.writeObject(output, src);
      output.close();
      input = new Input(bos.toByteArray());
      return (SRC)kryo.readObject(input, src.getClass());
    } finally {
      IOUtils.closeQuietly(input);
      IOUtils.closeQuietly(bos);
    }
  }

  public int getLatencyLowerBound()
  {
    return latencyLowerBound;
  }

  public void setLatencyLowerBound(int latencyLowerBound)
  {
    this.latencyLowerBound = latencyLowerBound;
  }

  public int getLatencyUpperBound()
  {
    return latencyUpperBound;
  }

  public void setLatencyUpperBound(int latencyUpperBound)
  {
    this.latencyUpperBound = latencyUpperBound;
  }
}
