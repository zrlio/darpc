package com.ibm.darpc;

public interface DaRPCProtocol<R extends DaRPCMessage, T extends DaRPCMessage> {
	public abstract R createRequest();
	public abstract T createResponse();	
}
