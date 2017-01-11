package com.datatorrent.apps;

import com.datatorrent.lib.codec.KryoSerializableStreamCodec;
import com.datatorrent.lib.util.PojoUtils;

public class DeduperStreamCodec extends KryoSerializableStreamCodec
{
  PojoUtils.GetterInt<Object> accountNumber;

  @Override
  public int getPartition(Object o)
  {
    if (accountNumber == null) {
      accountNumber = PojoUtils.createGetterInt(o.getClass(), "accountNumber");
    }
    return accountNumber.get(o);
  }
}
