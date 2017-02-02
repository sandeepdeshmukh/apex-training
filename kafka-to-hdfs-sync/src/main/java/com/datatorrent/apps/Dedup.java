package com.datatorrent.apps;

import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.common.util.BaseOperator;

public class Dedup extends BaseOperator {
	
	private DedupStreamCodec streamCodec ;
	public final transient DefaultOutputPort<Object> output = new DefaultOutputPort<>();
	
	public final transient DefaultInputPort<Object> input = new DefaultInputPort<Object>() {
		public void process(Object tuple) {
			processTuple(tuple);
			
		};
		
		public com.datatorrent.api.StreamCodec<Object> getStreamCodec() {
			if(streamCodec == null){
				streamCodec = new DedupStreamCodec();
			}
			return streamCodec;
		};
	};
	
	protected void processTuple(Object tuple){
		output.emit(tuple);
	}
}
