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

import java.util.concurrent.Future;

public abstract class RpcFuture<R extends RdmaRpcMessage, T extends RdmaRpcMessage> implements Future<T> {
	protected static int RPC_PENDING = 0;
	protected static int RPC_DONE = 1;
	protected static int RPC_ERROR = 2;	
	
	private int ticket;
	private R request;
	private T response;

	public abstract void signal(int status);
	
	public RpcFuture(R request, T response){
		this.request = request;
		this.response = response;		
		this.ticket = -1;
	}
	
	public int getTicket() {
		return this.ticket;
	}
	
	public R getRequest(){
		return request;
	}
	
	public T getResponse(){
		return response;
	}
	
	public void stamp(int ticket) {
		this.ticket = ticket;
	}
}
