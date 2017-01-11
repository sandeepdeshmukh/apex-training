package com.datatorrent.apps;

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
import com.datatorrent.api.Operator;
import com.datatorrent.api.Partitioner;
import com.datatorrent.api.StatsListener;
import com.datatorrent.api.StreamCodec;
import com.datatorrent.common.util.BaseOperator;
import com.datatorrent.lib.util.PojoUtils;

public class Deduper extends BaseOperator implements Operator.ActivationListener, Partitioner<Deduper>, StatsListener
{
  private transient Class<?> clazz;
  private transient PojoUtils.GetterInt<Object> accountNumber;
  private Set<Integer> dedupSet = new HashSet<>();
  private transient StreamCodec streamCodec;

  private static final int MAX_PARTITIONS = 2;
  private static final int MIN_PARTITIONS = 1;
  private static final long COOL_OFF_MILLIS = 30*1000;
  private int numPartitions = 1;
  private long lastRepartitionTime = System.currentTimeMillis();
  private Set<Integer> partitionKeys;
  private int partitionMask;
  private int latencyLowerBound = 1;
  private int latencyUpperBound = 10;

  public final transient DefaultInputPort<Object> input = new DefaultInputPort<Object>()
  {
    @Override
    public void process(Object o)
    {
      processTuple(o);
    }

    @Override
    public void setup(Context.PortContext context)
    {
      clazz = context.getAttributes().get(Context.PortContext.TUPLE_CLASS);
      partitionKeys = Sets.newHashSet(0);
      partitionMask = 0;
    }

    @Override
    public StreamCodec<Object> getStreamCodec()
    {
      if (streamCodec == null) {
        streamCodec = new DeduperStreamCodec();
      }
      return streamCodec;
    }
  };

  public final transient DefaultOutputPort<Object> unique = new DefaultOutputPort<>();

  private void processTuple(Object o)
  {
    int i = accountNumber.get(o);
    if (!dedupSet.contains(i)) {
      unique.emit(o);
      dedupSet.add(i);
    }
  }

  @Override
  public void activate(Context context)
  {
    accountNumber = PojoUtils.createGetterInt(clazz, "accountNumber");
  }

  @Override
  public void deactivate()
  {
    // Do nothing
  }

  @Override
  public Collection<Partition<Deduper>> definePartitions(Collection<Partition<Deduper>> partitions, PartitioningContext context)
  {
    final int newPartitionCount = DefaultPartition.getRequiredPartitionCount(context, this.numPartitions);

    if(newPartitionCount <= 0) {
      throw new RuntimeException("Dynamic Partition - Number of partitions should be >= 1");
    }

    int oldPartitions = partitions.size();

    if (oldPartitions == newPartitionCount) {
      LOG.info("Dynamic Partition - Nothing to do in definePartitions");
      return partitions;
    }
    LOG.debug("Dynamic Partition - Repartitioning from {} to {}", oldPartitions, newPartitionCount);

    // Collect old state
    HashSet<Integer> state = Sets.newHashSet();
    for (Partition<Deduper> partition: partitions) {
      state.addAll(partition.getPartitionedInstance().dedupSet);
    }
    LOG.info("Dynamic Partition - Old State: " + state);

    Kryo kryo = new Kryo();
    List<Partition<Deduper>> newPartitions = Lists.newArrayListWithExpectedSize(newPartitionCount);

    for (int i = 0; i < newPartitionCount; i++) {
      Deduper operator = cloneObject(kryo, this);
      newPartitions.add(new DefaultPartition<>(operator));
    }

    // Assign partition keys
    List<InputPort<?>> inputPortList = context.getInputPorts();
    InputPort<?> inputPort = inputPortList.iterator().next();
    DefaultPartition.assignPartitionKeys(Collections.unmodifiableCollection(newPartitions), inputPort);
    int commonMask = newPartitions.iterator().next().getPartitionKeys().get(inputPort).mask;

    // distribute state to new partitions
    for (Partition<Deduper> dedupPartition: newPartitions) {
      Deduper dedupInstance = dedupPartition.getPartitionedInstance();
      dedupInstance.partitionKeys = dedupPartition.getPartitionKeys().get(inputPort).partitions;
      dedupInstance.partitionMask = commonMask;

      for (int key: state) {
        int partitionKey = Integer.valueOf(key).hashCode() & commonMask;
        if (dedupInstance.partitionKeys.contains(partitionKey)) {
          dedupInstance.dedupSet.add(key);
        }
      }
      LOG.info("Dynamic Partition - Partition State: " + dedupInstance.dedupSet);
    }
    return newPartitions;
  }

  @Override
  public void partitioned(Map<Integer, Partition<Deduper>> partitions)
  {
    if (numPartitions != partitions.size()) {
      String msg = String.format("Dynamic Partition - partitions = %d, map.size = %d%n", partitions, partitions.size());
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
      LOG.info("Dynamic Partition - Latency: " + latency);
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

  private static final Logger LOG = LoggerFactory.getLogger(Deduper.class);
}
