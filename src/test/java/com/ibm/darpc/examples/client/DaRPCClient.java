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

package com.ibm.darpc.examples.client;

import java.io.FileOutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;

import com.ibm.darpc.RpcActiveEndpointGroup;
import com.ibm.darpc.RpcClientEndpoint;
import com.ibm.darpc.RpcEndpointGroup;
import com.ibm.darpc.RpcFuture;
import com.ibm.darpc.RpcPassiveEndpointGroup;
import com.ibm.darpc.RpcStream;
import com.ibm.darpc.examples.protocol.RdmaRpcProtocol;
import com.ibm.darpc.examples.protocol.RdmaRpcProtocol.*;
import com.ibm.disni.util.*;

public class DaRPCClient {
	public static enum BenchmarkType {
		UNDEFINED
	};	
	
	public static class ClientThread implements Runnable {
		public static final int FUTURE_POLL = 0;
		public static final int STREAM_POLL = 1;
		public static final int FUTURE_TAKE = 2;
		public static final int STREAM_TAKE = 3;
		public static final int BATCH_STREAM_TAKE = 4;
		public static final int BATCH_STREAM_POLL = 5;
		
		private RpcClientEndpoint<RdmaRpcRequest, RdmaRpcResponse> clientEp;
		private int loop;
		private int queryMode;
		private int rpcpipeline;
		private int clienttimeout;
		
		protected double throughput;
		protected double latency;
		protected double readOps;
		protected double writeOps;
		protected double errorOps;		
		private double ops;		
		
		public ClientThread(RpcClientEndpoint<RdmaRpcRequest, RdmaRpcResponse> clientEp, int loop, InetSocketAddress address, int mode, int rpcpipeline, int clienttimeout){
			this.clientEp = clientEp;
			this.loop = loop;
			this.queryMode = mode;
			this.rpcpipeline = rpcpipeline;
			this.clienttimeout = clienttimeout;
		}
		
		@Override
		public void run() {
			try {
				System.out.println("starting3, eid " + clientEp.getEndpointId() + ", mode2 " + queryMode + ", rpcpipeline " + rpcpipeline);
				RpcStream<RdmaRpcRequest, RdmaRpcResponse> stream = clientEp.createStream();
				RdmaRpcRequest request = new RdmaRpcRequest();
				ArrayBlockingQueue<RdmaRpcResponse> freeResponses = new ArrayBlockingQueue<RdmaRpcResponse>(rpcpipeline);
				this.ops = 0.0;	
				int k = 1;
				for (int i = 1; i <= loop; i++) {
					RdmaRpcResponse response = freeResponses.poll();
					if (response == null){
						response = new RdmaRpcResponse();
					}
					request.setParam(i);
					if (response.getName() == request.getParam() + 1){
						System.out.println("############## wrong RPC response init value!!");
					}
					RpcFuture<RdmaRpcRequest, RdmaRpcResponse> future = stream.request(request, response, true);
					
					switch (queryMode) {
					case FUTURE_POLL:
						while (!future.isDone()) {
						}
						if (future.getReceiveMessage().getName() != future.getSendMessage().getParam() + 1){
							System.out.println("############## wrong RPC return value!!");
						}
						freeResponses.add(future.getReceiveMessage());
						stream.clear();
						break;
					case STREAM_POLL:
						future = stream.poll();
						while (future == null) {
							future = stream.poll();
						}
						freeResponses.add(future.getReceiveMessage());
						stream.clear();
						break;		
					case FUTURE_TAKE:
						if (future.get(clienttimeout, TimeUnit.MILLISECONDS) != null){
							RdmaRpcResponse val = future.getReceiveMessage();
							freeResponses.add(future.getReceiveMessage());
						} else {
							System.out.println("invalid value");
						}
						stream.clear();
						break;						
					case STREAM_TAKE:
						future = stream.take(clienttimeout);
						if (future != null){
							freeResponses.add(future.getReceiveMessage());
						} 
						stream.clear();
						break;
					case BATCH_STREAM_TAKE:
						if ((i > 0) && ((i % rpcpipeline) == 0)) {
							for (int j = 1; j <= rpcpipeline; j++) {
								future = stream.take();
								System.out.println("i " + i + ", k " + k + ", response " + future.getReceiveMessage().toString());
								freeResponses.add(future.getReceiveMessage());
								k++;
							}
							stream.clear();
						}
						break;
					case BATCH_STREAM_POLL:
						if ((i > 0) && ((i % rpcpipeline) == 0)) {
							for (int j = 1; j <= rpcpipeline; j++) {
								future = stream.poll();
								while (future == null){
									future = stream.poll();
								}
								System.out.println("i " + i + ", k " + k + ", response " + future.getReceiveMessage().toString());
								freeResponses.add(future.getReceiveMessage());
								k++;
							}
							stream.clear();
						}
						break;						
					}
					ops += 1.0;
				}
			} catch(Exception e){
				e.printStackTrace();
			}
		}

		public void close() throws Exception {
			clientEp.close();
		}
		
		public double getThroughput() {
			return throughput;
		}

		public double getLatency() {
			return this.latency;
		}

		public double getReadOps() {
			return this.readOps;
		}

		public double getWriteOps() {
			return this.writeOps;
		}

		public double getErrorOps() {
			return this.errorOps;
		}	
		
		public double getOps(){
			return ops;
		}		
	}
	
	public void launch(String[] args) throws Exception {
		String ipAddress = ""; 
		int size = 24;
		int loop = 100;
		int threadCount = 1;
		int mode = ClientThread.FUTURE_POLL;
		boolean shared = false;
		int rpcpipeline = 100;
		int poolsize = 3;
		int connections = 1;
		int clienttimeout = 3000;
		int maxinline = 0;

		String[] _args = args;
		if (args.length < 1) {
			System.exit(0);
		} else if (args[0].equals(DaRPCClient.class.getCanonicalName())) {
			_args = new String[args.length - 1];
			for (int i = 0; i < _args.length; i++) {
				_args[i] = args[i + 1];
			}
		}

		GetOpt go = new GetOpt(_args, "a:s:k:n:m:hr:p:c:t:i:");
		go.optErr = true;
		int ch = -1;
		
		System.setProperty("com.ibm.jverbs.provider", "nat");
		System.setProperty("com.ibm.jverbs.driver", "siw");

		while ((ch = go.getopt()) != GetOpt.optEOF) {
			if ((char) ch == 'a') {
				ipAddress = go.optArgGet();
			} else if ((char) ch == 's') {
				int serialized_size = Integer.parseInt(go.optArgGet());
				RdmaRpcProtocol.RdmaRpcRequest.SERIALIZED_SIZE = serialized_size;
				RdmaRpcProtocol.RdmaRpcResponse.SERIALIZED_SIZE = serialized_size;
			} else if ((char) ch == 'k') {
				loop = Integer.parseInt(go.optArgGet());
			} else if ((char) ch == 'n') {
				threadCount = Integer.parseInt(go.optArgGet());
			} else if ((char) ch == 'm') {
				String _mode = go.optArgGet();
				if (_mode.equalsIgnoreCase("future-poll")) {
					mode = ClientThread.FUTURE_POLL;
				} else if (_mode.equalsIgnoreCase("stream-poll")) {
					mode = ClientThread.STREAM_POLL;
				} else if (_mode.equalsIgnoreCase("future-take")) {
					mode = ClientThread.FUTURE_TAKE;
				} else if (_mode.equalsIgnoreCase("stream-take")) {
					mode = ClientThread.STREAM_TAKE;
				} else if (_mode.equalsIgnoreCase("batch-stream-take")) {
					mode = ClientThread.BATCH_STREAM_TAKE;
				} else if (_mode.equalsIgnoreCase("batch-stream-poll")) {
					mode = ClientThread.BATCH_STREAM_POLL;
				}
			} else if ((char) ch == 'h') {
				shared = true;
			}  else if ((char) ch == 'r') {
				rpcpipeline = Integer.parseInt(go.optArgGet());
			} else if ((char) ch == 'p') {
				poolsize = Integer.parseInt(go.optArgGet());
			} else if ((char) ch == 'c') {
				connections = Integer.parseInt(go.optArgGet());
			} else if ((char) ch == 't') {
				clienttimeout = Integer.parseInt(go.optArgGet());
			} else if ((char) ch == 'i') {
				maxinline = Integer.parseInt(go.optArgGet());
			} else {
				System.exit(1); // undefined option
			}
		}	
		
		long[] clusterAffinities;
		if (poolsize == 1){
			long _clusterAffinities[] = { 1L << 1 | 1L << 17};
			clusterAffinities = _clusterAffinities;
		} else if (poolsize == 2){
			long _clusterAffinities[] = { 1L << 1 | 1L << 17, 1L << 2 | 1L << 18, 1L << 3 | 1L << 19, 1L << 4 | 1L << 20};
			clusterAffinities = _clusterAffinities;
		} else if (poolsize == 3){
			long _clusterAffinities[] = { 1L << 9 | 1L << 25, 1L << 10 | 1L << 26, 1L << 11 | 1L << 27, 1L << 12 | 1L << 28};
			clusterAffinities = _clusterAffinities;
		} else if (poolsize == 4){
			long _clusterAffinities[] = {1L << 1 | 1L << 17, 1L << 2 | 1L << 18, 1L << 3 | 1L << 19, 1L << 4 | 1L << 20, 1L << 9 | 1L << 25, 1L << 10 | 1L << 26, 1L << 11 | 1L << 27, 1L << 12 | 1L << 28};
			clusterAffinities = _clusterAffinities;
		} else if (poolsize == 5){
			long _clusterAffinities[] = { 1L << 1 | 1L << 17, 1L << 2 | 1L << 18, 1L << 3 | 1L << 19, 1L << 4 | 1L << 20};
			clusterAffinities = _clusterAffinities;
		} else {
			long _clusterAffinities[] = { 1L << 1 | 1L << 17 };
			clusterAffinities = _clusterAffinities;
		}	
		System.out.println("poolsize " + poolsize + ", affinity size " + clusterAffinities.length);
		
		if ((threadCount % connections) != 0){
			throw new Exception("thread count needs to be a multiple of connections");
		}
		
		int threadsperconnection = threadCount / connections;
		
		RpcClientEndpoint<?,?>[] rpcConnections = new RpcClientEndpoint[connections];
		System.out.println("connections " + rpcConnections.length);
		Thread[] workers = new Thread[threadCount];
		System.out.println("total threads " + workers.length);
		ClientThread[] benchmarkTask = new ClientThread[threadCount];
		
		InetAddress localHost = InetAddress.getByName(ipAddress);
		InetSocketAddress address = new InetSocketAddress(localHost, 1919);		
		RdmaRpcProtocol rpcProtocol = new RdmaRpcProtocol();
		RpcEndpointGroup<RdmaRpcProtocol.RdmaRpcRequest, RdmaRpcProtocol.RdmaRpcResponse> group;
		if (shared){
			group = RpcActiveEndpointGroup.createDefault(rpcProtocol, clusterAffinities, 100, maxinline, false, rpcpipeline, 4, rpcpipeline);
		} else {
			group = RpcPassiveEndpointGroup.createDefault(rpcProtocol, clusterAffinities, 100, maxinline, false, rpcpipeline, 4, rpcpipeline);
		}
		
		int k = 0;
		for (int i = 0; i < rpcConnections.length; i++){
			System.out.println("starting connection " + i);
			RpcClientEndpoint<RdmaRpcProtocol.RdmaRpcRequest, RdmaRpcProtocol.RdmaRpcResponse> clientEp = group.createEndpoint();
			clientEp.connect(address, 1000);
			rpcConnections[i] = clientEp;
			
			for (int j = 0; j < threadsperconnection; j++){
				System.out.println("starting thread " + j);
				benchmarkTask[k] = new ClientThread(clientEp, loop, address, mode, rpcpipeline, clienttimeout);
				k++;
			}
		}

		StopWatch stopWatchThroughput = new StopWatch();
		stopWatchThroughput.start();		
		for(int i = 0; i < threadCount;i++){
			workers[i] = new Thread(benchmarkTask[i]);
			workers[i].start();
		}
		for(int i = 0; i < threadCount;i++){
			workers[i].join();
			System.out.println("finished joining worker " + i);
		}
		double executionTime = (double) stopWatchThroughput.getExecutionTime() / 1000.0;
		System.out.println("executionTime " + executionTime);
		double ops = 0;
		for (int i = 0; i < threadCount; i++) {
			ops += benchmarkTask[i].getOps();
		}
		System.out.println("ops " + ops);
		double throughput = 0.0;
		double latency = 0.0;
		if (executionTime > 0) {
			throughput = ops / executionTime;
			double _threadcount = (double) threadCount;
			double throughputperclient = throughput / _threadcount;
			double norm = 1.0;
			latency = norm / throughputperclient * 1000000.0;
		}		

		String dataFilename = "datalog-client.dat";
		FileOutputStream dataStream = new FileOutputStream(dataFilename, true);
		FileChannel dataChannel = dataStream.getChannel();
		String _bechmarkType = "";
		String logdata = _bechmarkType + "\t\t"
				+ 0 + "\t" + loop + "\t" + size
				+ "\t\t" + 0 + "\t\t"
				+ 0 + "\t\t"
				+ throughput + "\t\t"
				+ 9 + "\t\t"
				+ 0 + "\t\t" + latency
				+ "\n";
		ByteBuffer buffer = ByteBuffer.wrap(logdata.getBytes());
		System.out.println("writing data file for workers ");
		dataChannel.write(buffer);
		System.out.println("closing channel");
		dataChannel.close();		
		dataStream.close();
		
		for (int i = 0; i < rpcConnections.length; i++){
			rpcConnections[i].close();
		}
		group.close();
		
		System.out.println("done");
	}
	
	public static void main(String[] args) throws Exception { 
		DaRPCClient rpcClient = new DaRPCClient();
		rpcClient.launch(args);		
		System.exit(0);
	}
}
