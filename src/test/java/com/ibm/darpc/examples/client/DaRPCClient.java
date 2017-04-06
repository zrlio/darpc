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
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;

import com.ibm.darpc.DaRPCClientEndpoint;
import com.ibm.darpc.DaRPCClientGroup;
import com.ibm.darpc.DaRPCEndpoint;
import com.ibm.darpc.DaRPCFuture;
import com.ibm.darpc.DaRPCStream;
import com.ibm.darpc.examples.protocol.RdmaRpcProtocol;
import com.ibm.darpc.examples.protocol.RdmaRpcRequest;
import com.ibm.darpc.examples.protocol.RdmaRpcResponse;
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
		
		private DaRPCClientEndpoint<RdmaRpcRequest, RdmaRpcResponse> clientEp;
		private int loop;
		private int queryMode;
		private int clienttimeout;
		private ArrayBlockingQueue<RdmaRpcResponse> freeResponses;
		
		protected double throughput;
		protected double latency;
		protected double readOps;
		protected double writeOps;
		protected double errorOps;		
		
		public ClientThread(DaRPCClientEndpoint<RdmaRpcRequest, RdmaRpcResponse> clientEp, int loop, URI uri, int mode, int rpcpipeline, int clienttimeout){
			this.clientEp = clientEp;
			this.loop = loop;
			this.queryMode = mode;
			this.clienttimeout = clienttimeout;
			this.freeResponses = new ArrayBlockingQueue<RdmaRpcResponse>(rpcpipeline);
			for (int i = 0; i < rpcpipeline; i++){
				RdmaRpcResponse response = new RdmaRpcResponse();
				freeResponses.add(response);
			}	
		}
		
		@Override
		public void run() {
			try {
				DaRPCStream<RdmaRpcRequest, RdmaRpcResponse> stream = clientEp.createStream();
				RdmaRpcRequest request = new RdmaRpcRequest();
				boolean streamMode = (queryMode == STREAM_POLL) || (queryMode == STREAM_TAKE) || (queryMode == BATCH_STREAM_TAKE) || (queryMode == BATCH_STREAM_POLL);
				int issued = 0;
				int consumed = 0;
				for (;  issued < loop; issued++) {
					while(freeResponses.isEmpty()){
						DaRPCFuture<RdmaRpcRequest, RdmaRpcResponse> future = stream.poll();
						if (future != null){
							freeResponses.add(future.getReceiveMessage());						
							consumed++;
						}
					}
					
					request.setParam(issued);
					RdmaRpcResponse response = freeResponses.poll();
					DaRPCFuture<RdmaRpcRequest, RdmaRpcResponse> future = stream.request(request, response, streamMode);
					
					switch (queryMode) {
					case FUTURE_POLL:
						while (!future.isDone()) {
						}
						consumed++;
//						System.out.println("i " + issued + ", response " + future.getReceiveMessage().toString());
						freeResponses.add(future.getReceiveMessage());
						break;
					case STREAM_POLL:
						future = stream.poll();
						while (future == null) {
							future = stream.poll();
						}
						consumed++;
						freeResponses.add(future.getReceiveMessage());
						break;		
					case FUTURE_TAKE:
						future.get(clienttimeout, TimeUnit.MILLISECONDS);
						consumed++;
						freeResponses.add(future.getReceiveMessage());
						break;						
					case STREAM_TAKE:
						future = stream.take(clienttimeout);
						consumed++;
						freeResponses.add(future.getReceiveMessage());
						break;
					case BATCH_STREAM_TAKE:
						break;
					case BATCH_STREAM_POLL:
						break;						
					}
				}
				while (consumed < issued){
					DaRPCFuture<RdmaRpcRequest, RdmaRpcResponse> future = stream.take();
//					System.out.println("response " + future.getReceiveMessage().toString());
					consumed++;
					freeResponses.add(future.getReceiveMessage());
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
			return loop;
		}		
	}
	
	public void launch(String[] args) throws Exception {
		String ipAddress = ""; 
		int size = 24;
		int loop = 100;
		int threadCount = 1;
		int mode = ClientThread.FUTURE_POLL;
		int batchSize = 16;
		int connections = 1;
		int clienttimeout = 3000;
		int maxinline = 0;
		int recvQueue = batchSize;
		int sendQueue = batchSize;

		String[] _args = args;
		if (args.length < 1) {
			System.exit(0);
		} else if (args[0].equals(DaRPCClient.class.getCanonicalName())) {
			_args = new String[args.length - 1];
			for (int i = 0; i < _args.length; i++) {
				_args[i] = args[i + 1];
			}
		}

		GetOpt go = new GetOpt(_args, "a:k:n:m:b:c:a:r:s:l:");
		go.optErr = true;
		int ch = -1;
		
		while ((ch = go.getopt()) != GetOpt.optEOF) {
			if ((char) ch == 'a') {
				ipAddress = go.optArgGet();
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
			} else if ((char) ch == 'b') {
				batchSize = Integer.parseInt(go.optArgGet());
			} else if ((char) ch == 'c') {
				connections = Integer.parseInt(go.optArgGet());
			} else if ((char) ch == 't') {
				clienttimeout = Integer.parseInt(go.optArgGet());
			} else if ((char) ch == 'a') {
				maxinline = Integer.parseInt(go.optArgGet());
			} else if ((char) ch == 'r') {
				recvQueue = Integer.parseInt(go.optArgGet());
			} else if ((char) ch == 's') {
				sendQueue = Integer.parseInt(go.optArgGet());
			} else if ((char) ch == 'l') {
				RdmaRpcRequest.SERIALIZED_SIZE = Integer.parseInt(go.optArgGet());
				RdmaRpcResponse.SERIALIZED_SIZE = RdmaRpcRequest.SERIALIZED_SIZE;
			} else {
				System.exit(1); // undefined option
			}
		}	
		if ((threadCount % connections) != 0){
			throw new Exception("thread count needs to be a multiple of connections");
		}
		
		int threadsperconnection = threadCount / connections;
		DaRPCEndpoint<?,?>[] rpcConnections = new DaRPCEndpoint[connections];
		Thread[] workers = new Thread[threadCount];
		ClientThread[] benchmarkTask = new ClientThread[threadCount];
		
		RdmaRpcProtocol rpcProtocol = new RdmaRpcProtocol();
		System.out.println("starting.. threads " + threadCount + ", connections " + connections + ", server " + ipAddress + ", recvQueue " + recvQueue + ", sendQueue" + sendQueue + ", batchSize " + batchSize + ", mode " + mode);
		DaRPCClientGroup<RdmaRpcRequest, RdmaRpcResponse> group = DaRPCClientGroup.createClientGroup(rpcProtocol, 100, maxinline, recvQueue, sendQueue);
		URI uri = URI.create("rdma://" + ipAddress + ":" + 1919);
		int k = 0;
		for (int i = 0; i < rpcConnections.length; i++){
			DaRPCClientEndpoint<RdmaRpcRequest, RdmaRpcResponse> clientEp = group.createEndpoint();
			clientEp.connect(uri);
			rpcConnections[i] = clientEp;
			for (int j = 0; j < threadsperconnection; j++){
				benchmarkTask[k] = new ClientThread(clientEp, loop, uri, mode, batchSize, clienttimeout);
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
		System.out.println("throughput " + throughput);

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
		dataChannel.write(buffer);
		dataChannel.close();		
		dataStream.close();
		
		for (int i = 0; i < rpcConnections.length; i++){
			rpcConnections[i].close();
		}
		group.close();
	}
	
	public static void main(String[] args) throws Exception { 
		DaRPCClient rpcClient = new DaRPCClient();
		rpcClient.launch(args);		
		System.exit(0);
	}
}
