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
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RpcStreamActive <R extends RdmaRpcMessage, T extends RdmaRpcMessage> extends RpcStream<R,T> {
	private static final Logger logger = LoggerFactory.getLogger("com.ibm.darpc");
	
	private RpcActiveClientEndpoint<R,T> endpoint;
	private LinkedBlockingDeque<RpcFutureActive<R,T>> completedList;
	
	protected RpcStreamActive(RpcActiveClientEndpoint<R,T> endpoint, int streamId){
		logger.info("new shared rpc stream");
		this.endpoint = endpoint;
		this.completedList = new LinkedBlockingDeque<RpcFutureActive<R,T>>();
	}	
	
	public RpcFuture<R, T> request(R request, T response, boolean streamLogged) throws IOException {
		RpcFutureActive<R, T> future = new RpcFutureActive<R, T>(this, request, response, streamLogged);
		endpoint.sendRequest(future);
		return future;
	}	
	
	public RpcFuture<R, T> poll() throws IOException {
		return completedList.poll();
	}
	
	public RpcFuture<R, T> take() throws IOException {
		try {
			return completedList.take();
		} catch (InterruptedException e) {
			throw new IOException(e);
		}
	}
	
	public RpcFuture<R, T> take(int timeout) throws IOException {
		try {
			if (timeout < 0){
				return take();
			} 
			return completedList.poll(timeout, TimeUnit.MILLISECONDS);
		} catch(InterruptedException e){
			throw new IOException(e);
		}		
	}
	
	public void clear(){
		completedList.clear();
	}
	
	void addFuture(RpcFutureActive<R, T> future){
		completedList.add(future);
	}
}
