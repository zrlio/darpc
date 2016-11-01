package com.ibm.darpc.examples.protocol;

import java.nio.ByteBuffer;

import com.ibm.darpc.RpcMessage;

public class RdmaRpcResponse implements RpcMessage {
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