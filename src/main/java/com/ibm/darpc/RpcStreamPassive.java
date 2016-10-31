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

package com.ibm.darpc;

import java.io.IOException;
import java.util.concurrent.LinkedBlockingDeque;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ibm.disni.rdma.*;

public class RpcStreamPassive <R extends RdmaRpcMessage, T extends RdmaRpcMessage> extends RpcStream<R,T> {
	private static final Logger logger = LoggerFactory.getLogger("com.ibm.darpc");
	
	private RpcPassiveClientEndpoint<R,T> endpoint;
	private LinkedBlockingDeque<RpcFuturePassive<R, T>> completedList;
	
	RpcStreamPassive(RpcPassiveClientEndpoint<R,T> endpoint, int streamId) throws IOException{
		if (endpoint.getCqProvider() instanceof RdmaCqProcessor){
			throw new IOException("direct rpc mode only permitted at client side");
		}
		logger.info("new direct rpc stream");
		this.endpoint = endpoint;
		this.completedList = new LinkedBlockingDeque<RpcFuturePassive<R, T>>();
	}	
	
	public RpcFuture<R, T> request(R request, T response, boolean streamLogged) throws IOException {
		RpcFuturePassive<R, T> future = new RpcFuturePassive<R, T>(this, endpoint, request, response, streamLogged);
		endpoint.sendRequest(future);
		return future;
	}
	
	public RpcFuture<R, T> take() throws IOException {
		try {
			RpcFuture<R, T> future = completedList.poll();
			while (future == null){
				endpoint.pollOnce();
				future = completedList.poll();
			}
			return future;
		} catch(Exception e){
			throw new IOException(e);
		}
	}
	
	public RpcFuture<R, T> take(int timeout) throws IOException {
		try {
			RpcFuture<R, T> future = completedList.poll();
			long sumtime = 0;
			while (future == null && sumtime < timeout){
				endpoint.pollOnce();
				future = completedList.poll();
			}
			return future;
		} catch (Exception e){
			throw new IOException(e);
		}
		
	}
	
	public RpcFuture<R, T> poll() throws IOException {
		RpcFuture<R, T> future = completedList.poll();
		if (future == null){
			endpoint.pollOnce();
			future = completedList.poll();			
		}
		return future;
	}
	
	public void clear(){
		completedList.clear();
	}

	void addFuture(RpcFuturePassive<R, T> future){
		completedList.add(future);
	}
}
