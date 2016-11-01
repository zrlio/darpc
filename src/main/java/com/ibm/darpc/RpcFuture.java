package com.ibm.darpc;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RpcFuture<R extends RpcMessage, T extends RpcMessage> implements Future<T> {
	private static final Logger logger = LoggerFactory.getLogger("com.ibm.darpc");
	
	protected static int RPC_PENDING = 0;
	protected static int RPC_DONE = 1;
	protected static int RPC_ERROR = 2;	
	
	private int ticket;
	private R request;
	private T response;	
	
	private RpcStream<R, T> stream;
	private RpcClientEndpoint<R,T> endpoint;
	private boolean streamLogged;
	private AtomicInteger status;
	
	public RpcFuture(RpcStream<R,T> stream, RpcClientEndpoint<R,T> endpoint, R request, T response, boolean streamLogged){
		this.request = request;
		this.response = response;		
		this.ticket = -1;		
		
		this.stream = stream;
		this.endpoint = endpoint;
		this.status = new AtomicInteger(RPC_PENDING);
		this.streamLogged = streamLogged;
	}	
	
	public int getTicket() {
		return this.ticket;
	}
	
	public R getSendMessage(){
		return request;
	}
	
	public T getReceiveMessage(){
		return response;
	}
	
	public void stamp(int ticket) {
		this.ticket = ticket;
	}	
	
	@Override
	public T get() throws InterruptedException, ExecutionException {
		if (status.get() == RPC_PENDING){
			try {
				endpoint.pollUntil(status, Long.MAX_VALUE);
			} catch(Exception e){
				status.set(RPC_ERROR);
				throw new InterruptedException(e.getMessage());
			}
		}
		
		if (status.get() == RPC_DONE){
			return this.getReceiveMessage();
		} else if (status.get() == RPC_PENDING){
			throw new InterruptedException("RPC timeout");
		} else {
			throw new InterruptedException("RPC error");
		}
	}	
	
	@Override
	public T get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException {
		if (status.get() == RPC_PENDING){
			try {
				endpoint.pollUntil(status, timeout);
			} catch(Exception e){
				status.set(RPC_ERROR);
				throw new InterruptedException(e.getMessage());
			}
		}
		
		if (status.get() == RPC_DONE){
			return this.getReceiveMessage();
		} else if (status.get() == RPC_PENDING){
			throw new InterruptedException("RPC timeout");
		} else {
			throw new InterruptedException("RPC error");
		}
	}	
	
	@Override
	public boolean isDone() {
		if (status.get() == 0) {
			try {
				endpoint.pollOnce();
			} catch(Exception e){
				status.set(RPC_ERROR);
				logger.info(e.getMessage());
			}
		}
		return status.get() > 0;
	}

	public synchronized void signal(int wcstatus) {
		if (status.get() == 0){
			if (wcstatus == 0){
				status.set(RPC_DONE);
			} else {
				status.set(RPC_ERROR);
			}
			if (streamLogged){
				stream.addFuture(this);
			}
		}
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
	
	public boolean isStreamLogged() {
		return streamLogged;
	}
}
