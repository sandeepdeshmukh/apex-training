package com.datatorrent.apps;

import com.datatorrent.lib.codec.KryoSerializableStreamCodec;

public class DedupStreamCodec extends KryoSerializableStreamCodec<Object> {

	@Override
	public int getPartition(Object t) {
		PojoEvent t1 = (PojoEvent)t;
		return super.getPartition(t1.getAccountNumber());
	}
	
}
