package com.ibm.darpc;

public interface RpcProtocol<R extends RpcMessage, T extends RpcMessage> {
	public abstract R createRequest();
	public abstract T createResponse();	
}
