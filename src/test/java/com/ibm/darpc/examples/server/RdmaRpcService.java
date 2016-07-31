/*
 * DaRPC: Data Center Remote Procedure Call
 *
 * Author: Patrick Stuedi <stu@zurich.ibm.com>
 *
 * Copyright (C) 2016, IBM Corporation
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

import com.ibm.darpc.RpcServerEvent;
import com.ibm.darpc.examples.protocol.RdmaRpcProtocol;

public class RdmaRpcService extends RdmaRpcProtocol {
	private int servicetimeout;
	
	public RdmaRpcService(int servicetimeout){
		this.servicetimeout = servicetimeout;
		System.out.println("RpcService with timeout " + servicetimeout);
	}
	
	public void processServerEvent(RpcServerEvent<RdmaRpcProtocol.RdmaRpcRequest, RdmaRpcProtocol.RdmaRpcResponse> event) throws IOException {
		RdmaRpcProtocol.RdmaRpcRequest request = event.getRequest();
		RdmaRpcProtocol.RdmaRpcResponse response = event.getResponse();
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
	
}
