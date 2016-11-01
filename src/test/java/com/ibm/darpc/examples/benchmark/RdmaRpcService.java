package com.ibm.darpc.examples.benchmark;

import java.io.IOException;
import java.util.concurrent.LinkedBlockingQueue;

import com.ibm.darpc.RpcEndpoint;
import com.ibm.darpc.RpcServerEvent;
import com.ibm.darpc.RpcService;
import com.ibm.darpc.examples.benchmark.RdmaRpcProtocol;

public class RdmaRpcService extends RdmaRpcProtocol implements RpcService<RdmaRpcRequest, RdmaRpcResponse>, Runnable {
	public static boolean PARSE_FILENAME = false;
	public static boolean THREAD_POOL = false;
	
	private LinkedBlockingQueue<RpcServerEvent<RdmaRpcRequest, RdmaRpcResponse>> requestQueue;
	private boolean ready;
	
	public RdmaRpcService(){
		this.requestQueue = new LinkedBlockingQueue<RpcServerEvent<RdmaRpcRequest, RdmaRpcResponse>>();
		this.ready = false;
		Thread thread = new Thread(this);
		thread.start();
	}
	
	public void processServerEvent(RpcServerEvent<RdmaRpcRequest, RdmaRpcResponse> event) throws IOException {
		if (THREAD_POOL){
			try {
				synchronized(this){
					requestQueue.add(event);
					while(!ready){
						wait();
					}
//					System.out.println("event processed");
					ready = false;
				}
			} catch(Exception e){
				System.out.println("exception caught " + e.getMessage());
			}
		}
		event.triggerResponse();
	}

	@Override
	public void run() {
		System.out.println("starting event processing...");
		try {
			while(true){
				requestQueue.take();
				synchronized(this){
					ready = true;
					notify();
				}
			}
		} catch(Exception e){
			System.out.println("exception caught " + e.getMessage());
		}
	}

	@Override
	public void open(RpcEndpoint<RdmaRpcRequest, RdmaRpcResponse> rpcClientEndpoint) {
		
	}

	@Override
	public void close(RpcEndpoint<RdmaRpcRequest, RdmaRpcResponse> rpcClientEndpoint) {
		
	}
	
}
