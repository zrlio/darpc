package com.ibm.darpc.examples.benchmark;

import java.nio.ByteBuffer;

import com.ibm.darpc.RdmaRpcMessage;
import com.ibm.darpc.RdmaRpcService;
import com.ibm.darpc.examples.protocol.RdmaRpcProtocol.RdmaRpcRequest;
import com.ibm.darpc.examples.protocol.RdmaRpcProtocol.RdmaRpcResponse;

public class RdmaRpcProtocol extends RdmaRpcService<RdmaRpcProtocol.RdmaRpcRequest, RdmaRpcProtocol.RdmaRpcResponse> {
	public static final int FUNCTION_FOO = 1;
	public static final int FUNCTION_BAR = 2;	
	
	@Override
	public RdmaRpcProtocol.RdmaRpcRequest createRequest() {
		return new RdmaRpcRequest();
	}

	@Override
	public RdmaRpcProtocol.RdmaRpcResponse createResponse() {
		return new RdmaRpcResponse();
	}

	public static class RdmaRpcRequest implements RdmaRpcMessage {
		public static int SERIALIZED_SIZE = 16;
		
		private byte[] data;
		
		public RdmaRpcRequest(){
			data = new byte[SERIALIZED_SIZE];
			
			String filename = new String();
			for (int i = 0; i < SERIALIZED_SIZE; i++){
				filename += "X";
			}
			System.out.println("filename " + filename);
			byte barray[] = filename.getBytes();
			for (int i = 0; i < SERIALIZED_SIZE; i++){
				data[i] = barray[i];
			}			
		}

		public int size() {
			return SERIALIZED_SIZE;
		}

		@Override
		public void update(ByteBuffer buffer) {
			buffer.get(data);
		}

		@Override
		public int write(ByteBuffer buffer) {
			buffer.put(data);
			return SERIALIZED_SIZE;
		}
		
		public byte[] getData(){
			return data;
		}
			

	}
	
	public static class RdmaRpcResponse implements RdmaRpcMessage {
		public static int SERIALIZED_SIZE = 24;
		
		private int error;
		private int name;
		private long time;
		private int type;
		
		public RdmaRpcResponse(){
		}
		
		public int size() {
			return SERIALIZED_SIZE;
		}

		@Override
		public void update(ByteBuffer buffer) {
			error = buffer.getInt();
			name = buffer.getInt();
			time = buffer.getLong();
			type = buffer.getInt();
		}

		@Override
		public int write(ByteBuffer buffer) {
			buffer.putInt(error);
			buffer.putInt(name);
			buffer.putLong(time);
			buffer.putInt(type);
			
			return SERIALIZED_SIZE;
		}

		public int getError() {
			return error;
		}

		public int getName() {
			return name;
		}

		public long getTime() {
			return time;
		}

		public int getType() {
			return type;
		}

		public void setError(int error) {
			this.error = error;
		}

		public void setName(int name) {
			this.name = name;
		}

		public void setTime(long time) {
			this.time = time;
		}

		public void setType(int type) {
			this.type = type;
		}

		@Override
		public String toString() {
			return "" + name;
		}
	}
}
