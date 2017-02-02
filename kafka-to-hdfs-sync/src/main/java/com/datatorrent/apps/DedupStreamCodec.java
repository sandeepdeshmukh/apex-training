package com.datatorrent.apps;

import com.datatorrent.lib.codec.KryoSerializableStreamCodec;

public class DedupStreamCodec extends KryoSerializableStreamCodec<Object> {

	public int getPartition(Object t) {
		
		return ((PojoEvent)t).getAccountNumber();
	};
}
