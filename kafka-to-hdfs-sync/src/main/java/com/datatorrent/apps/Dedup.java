package com.datatorrent.apps;

import java.util.HashSet;
import java.util.Set;

import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.common.util.BaseOperator;

public class Dedup extends BaseOperator {
	
	private DedupStreamCodec streamCodec;
	private Set<Integer> set = new HashSet<Integer>();
	public final transient DefaultOutputPort<Object> unique = new DefaultOutputPort<>();
	public final transient DefaultOutputPort<Object> duplicate = new DefaultOutputPort<>();
	
	public final transient DefaultInputPort<Object> input = new DefaultInputPort<Object>() {

		@Override
		public void process(Object tuple) {
			PojoEvent t = (PojoEvent)tuple;
			if(set.contains(t.getAccountNumber())){
				// not a unique
				duplicate.emit(tuple);
				return;
			}
			// Unique
			unique.emit(tuple);
			set.add(t.getAccountNumber());
		}
		
		public com.datatorrent.api.StreamCodec<Object> getStreamCodec() {
			
			if(streamCodec == null){
				streamCodec = new DedupStreamCodec();
			}
			return streamCodec;
		};
		
	};
}
