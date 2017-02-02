package com.datatorrent.apps;

import java.util.HashSet;
import java.util.Set;

import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.common.util.BaseOperator;

public class Dedup extends BaseOperator {
	private Set<Integer> map = new HashSet<Integer>();
	private DedupStreamCodec streamCodec;
	public final transient DefaultOutputPort<Object> unique = new DefaultOutputPort<>();
	public final transient DefaultOutputPort<Object> duplicate = new DefaultOutputPort<>();

	public final transient DefaultInputPort<Object> input = new DefaultInputPort<Object>() {
		public void process(Object tuple) {
			processTuple(tuple);
		};

		public com.datatorrent.api.StreamCodec<Object> getStreamCodec() {
			if (streamCodec == null) {
				streamCodec = new DedupStreamCodec();
			}
			return streamCodec;
		};
	};

	protected void processTuple(Object tuple){
		PojoEvent t = (PojoEvent)tuple;
		if(map.contains(t.getAccountNumber())){
			duplicate.emit(tuple);
		}
		else{
			unique.emit(tuple);
			map.add(new Integer(t.getAccountNumber()));
		}
	}
}
