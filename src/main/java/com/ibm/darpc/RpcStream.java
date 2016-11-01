package com.ibm.darpc;

import java.io.IOException;
import java.util.concurrent.LinkedBlockingDeque;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RpcStream<R extends RpcMessage,T extends RpcMessage> {
	private static final Logger logger = LoggerFactory.getLogger("com.ibm.darpc");
	
	private RpcClientEndpoint<R,T> endpoint;
	private LinkedBlockingDeque<RpcFuture<R, T>> completedList;
	
	RpcStream(RpcClientEndpoint<R,T> endpoint, int streamId) throws IOException{
		logger.info("new direct rpc stream");
		this.endpoint = endpoint;
		this.completedList = new LinkedBlockingDeque<RpcFuture<R, T>>();
	}	
	
	public RpcFuture<R, T> request(R request, T response, boolean streamLogged) throws IOException {
		RpcFuture<R, T> future = new RpcFuture<R, T>(this, endpoint, request, response, streamLogged);
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

	void addFuture(RpcFuture<R, T> future){
		completedList.add(future);
	}
}
