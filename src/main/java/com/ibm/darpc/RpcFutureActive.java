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

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class RpcFutureActive<R extends RdmaRpcMessage, T extends RdmaRpcMessage> extends RpcFuture<R,T> {
	private RpcStreamActive<R,T> stream;
	private boolean streamLogged;
	private AtomicInteger status;
	
	public RpcFutureActive(RpcStreamActive<R,T> stream, R request, T response, boolean streamLogged){
		super(request, response);
		this.stream = stream;
		this.status = new AtomicInteger(RPC_PENDING);
		this.streamLogged = streamLogged;
	}	
	
	@Override
	public synchronized T get() throws InterruptedException, ExecutionException {
		if (status.get() == RPC_PENDING){
			try {
				wait();
			} catch (Exception e) {
				status.set(RPC_ERROR);
				throw new InterruptedException(e.getMessage());
			}
		}
		
		if (status.get() == RPC_DONE){
			return this.getResponse();
		} else if (status.get() == RPC_PENDING){
			throw new InterruptedException("RPC timeout");
		} else {
			throw new InterruptedException("RPC error");
		}
	}
	
	@Override
	public synchronized T get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException {
		if (status.get() == RPC_PENDING){
			try {
				wait(timeout);
			} catch (Exception e) {
				status.set(RPC_ERROR);
				throw new InterruptedException(e.getMessage());
			}
		}
		
		if (status.get() == RPC_DONE){
			return this.getResponse();
		} else if (status.get() == RPC_PENDING){
			throw new InterruptedException("RPC timeout");
		} else {
			throw new InterruptedException("RPC error");
		}
	}
	
	public boolean isDone() {
		return status.get() > 0;
	}	
	
	public synchronized void signal(int wcstatus){
		if (status.get() == 0){
			if (wcstatus == 0){
				status.set(RPC_DONE);
			} else {
				status.set(RPC_ERROR);
			}
			if (streamLogged){
				stream.addFuture(this);
			}
			notify();
		}
	}
	
	public boolean isStreamLogged() {
		return streamLogged;
	}

	@Override
	public boolean cancel(boolean mayInterruptIfRunning) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean isCancelled() {
		// TODO Auto-generated method stub
		return false;
	}
}
