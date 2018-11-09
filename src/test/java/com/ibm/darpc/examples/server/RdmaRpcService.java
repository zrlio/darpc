/*
 * DaRPC: Data Center Remote Procedure Call
 *
 * Author: Patrick Stuedi <stu@zurich.ibm.com>
 *
 * Copyright (C) 2016-2018, IBM Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package com.ibm.darpc.examples.server;

import java.io.IOException;

import com.ibm.darpc.DaRPCServerEndpoint;
import com.ibm.darpc.DaRPCServerEvent;
import com.ibm.darpc.DaRPCService;
import com.ibm.darpc.examples.protocol.RdmaRpcProtocol;
import com.ibm.darpc.examples.protocol.RdmaRpcRequest;
import com.ibm.darpc.examples.protocol.RdmaRpcResponse;

public class RdmaRpcService extends RdmaRpcProtocol implements DaRPCService<RdmaRpcRequest, RdmaRpcResponse> {
	private int servicetimeout;
	
	public RdmaRpcService(int servicetimeout){
		this.servicetimeout = servicetimeout;
	}
	
	public void processServerEvent(DaRPCServerEvent<RdmaRpcRequest, RdmaRpcResponse> event) throws IOException {
		RdmaRpcRequest request = event.getReceiveMessage();
		RdmaRpcResponse response = event.getSendMessage();
		response.setName(request.getParam() + 1);
		if (servicetimeout > 0){
			try {
				Thread.sleep(servicetimeout);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		event.triggerResponse();
	}

	@Override
	public void open(DaRPCServerEndpoint<RdmaRpcRequest, RdmaRpcResponse> endpoint) {
		System.out.println("new connection " + endpoint.getEndpointId() + ", cluster " + endpoint.clusterId());
	}

	@Override
	public void close(DaRPCServerEndpoint<RdmaRpcRequest, RdmaRpcResponse> endpoint) {
		System.out.println("disconnecting " + endpoint.getEndpointId() + ", cluster " + endpoint.clusterId());
	}
}
