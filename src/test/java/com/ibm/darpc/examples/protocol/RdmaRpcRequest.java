package com.ibm.darpc.examples.protocol;

import java.nio.ByteBuffer;

import com.ibm.darpc.RpcMessage;

public class RdmaRpcRequest implements RpcMessage {
	public static int SERIALIZED_SIZE = 16;
	
	private int cmd;
	private int param;
	private long time;
	
	public RdmaRpcRequest(){
	}

	public int size() {
		return SERIALIZED_SIZE;
	}

	@Override
	public void update(ByteBuffer buffer) {
		cmd = buffer.getInt();
		param = buffer.getInt();
		time = buffer.getLong();
	}

	@Override
	public int write(ByteBuffer buffer) {
		buffer.putInt(cmd);
		buffer.putInt(param);
		buffer.putLong(time);
		
		return SERIALIZED_SIZE;
	}

	public int getCmd() {
		return cmd;
	}

	public void setCmd(int cmd) {
		this.cmd = cmd;
	}

	public int getParam() {
		return param;
	}

	public void setParam(int param) {
		this.param = param;
	}
}