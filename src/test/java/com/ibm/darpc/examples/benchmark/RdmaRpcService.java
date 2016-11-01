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
		RdmaRpcRequest request = event.getReceiveMessage();
		RdmaRpcResponse response = event.getSendMessage();
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
				RpcServerEvent<RdmaRpcRequest, RdmaRpcResponse> event = requestQueue.take();
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

	@Override
	public void open(RpcEndpoint rpcClientEndpoint) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void close(RpcEndpoint rpcClientEndpoint) {
		// TODO Auto-generated method stub
		
	}
	
}
