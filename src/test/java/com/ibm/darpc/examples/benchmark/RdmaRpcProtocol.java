package com.ibm.darpc.examples.benchmark;

import com.ibm.darpc.RpcProtocol;

public class RdmaRpcProtocol implements RpcProtocol<RdmaRpcRequest, RdmaRpcResponse> {
	public static final int FUNCTION_FOO = 1;
	public static final int FUNCTION_BAR = 2;	
	
	@Override
	public RdmaRpcRequest createRequest() {
		return new RdmaRpcRequest();
	}

	@Override
	public RdmaRpcResponse createResponse() {
		return new RdmaRpcResponse();
	}
}
