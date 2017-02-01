package com.datatorrent.apps.solution;

import com.datatorrent.api.Context;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.Operator;
import com.datatorrent.common.util.BaseOperator;
import com.datatorrent.lib.util.PojoUtils;

import java.util.HashSet;
import java.util.Set;

public class Deduper extends BaseOperator implements Operator.ActivationListener
{
  private transient Class<?> clazz;
  private transient PojoUtils.GetterInt<Object> accountNumber;
  private transient Set<Integer> dedupSet = new HashSet<>();

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
}
