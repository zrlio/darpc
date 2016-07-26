package com.ibm.darpc.examples.benchmark;

import java.io.IOException;
import java.util.concurrent.LinkedBlockingQueue;

import com.ibm.darpc.RpcServerEvent;
import com.ibm.darpc.examples.benchmark.RdmaRpcProtocol;

public class RdmaRpcService extends RdmaRpcProtocol implements Runnable {
	public static boolean PARSE_FILENAME = false;
	public static boolean THREAD_POOL = false;
	
	private LinkedBlockingQueue<RpcServerEvent<RdmaRpcProtocol.RdmaRpcRequest, RdmaRpcProtocol.RdmaRpcResponse>> requestQueue;
	private boolean ready;
	
	public RdmaRpcService(){
		this.requestQueue = new LinkedBlockingQueue<RpcServerEvent<RdmaRpcProtocol.RdmaRpcRequest, RdmaRpcProtocol.RdmaRpcResponse>>();
		this.ready = false;
		Thread thread = new Thread(this);
		thread.start();
	}
	
	public void processServerEvent(RpcServerEvent<RdmaRpcProtocol.RdmaRpcRequest, RdmaRpcProtocol.RdmaRpcResponse> event) throws IOException {
		RdmaRpcProtocol.RdmaRpcRequest request = event.getRequest();
		RdmaRpcProtocol.RdmaRpcResponse response = event.getResponse();
//		System.out.println("got rpc request, size " + request.getData().length);
		if (PARSE_FILENAME){
			String filename = new String(request.getData());
//			System.out.println("got rpc request, filename " + filename);
		} 
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
				RpcServerEvent<RdmaRpcProtocol.RdmaRpcRequest, RdmaRpcProtocol.RdmaRpcResponse> event = requestQueue.take();
				synchronized(this){
//					System.out.println("got event");
					ready = true;
					notify();
				}
			}
		} catch(Exception e){
			System.out.println("exception caught " + e.getMessage());
		}
	}
	
}
