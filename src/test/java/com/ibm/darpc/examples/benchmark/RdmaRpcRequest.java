package com.ibm.darpc.examples.benchmark;

import java.nio.ByteBuffer;

import com.ibm.darpc.RpcMessage;

public class RdmaRpcRequest implements RpcMessage {
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