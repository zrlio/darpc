/*
 * DaRPC: Data Center Remote Procedure Call
 *
 * Author: Patrick Stuedi <stu@zurich.ibm.com>
 *
 * Copyright (C) 2016-2018, IBM Corporation
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
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

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
		
		public ClientThread(DaRPCClientEndpoint<RdmaRpcRequest, RdmaRpcResponse> clientEp, int loop, InetSocketAddress address, int mode, int rpcpipeline, int clienttimeout){
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
		String host = ""; 
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

		Option addressOption = Option.builder("a").required().desc("server address").hasArg().build();
		Option loopOption = Option.builder("k").desc("loop count").hasArg().build();
		Option threadCountOption = Option.builder("n").desc("thread count").hasArg().build();
		Option modeOption = Option.builder("m").desc("mode").hasArg().build();
		Option batchSizeOption = Option.builder("b").desc("batch size").hasArg().build();
		Option connectionsOption = Option.builder("c").desc("number of connections").hasArg().build();
		Option clienttimeoutOption = Option.builder("t").desc("client timeout").hasArg().build();
		Option maxinlineOption = Option.builder("i").desc("max inline data").hasArg().build();
		Option sendQueueOption = Option.builder("s").desc("send queue").hasArg().build();
		Option recvQueueOption = Option.builder("r").desc("receive queue").hasArg().build();
		Option serializedSizeOption = Option.builder("l").desc("serialized size").hasArg().build();
		Options options = new Options();
		options.addOption(addressOption);
		options.addOption(loopOption);
		options.addOption(threadCountOption);
		options.addOption(modeOption);
		options.addOption(batchSizeOption);
		options.addOption(connectionsOption);
		options.addOption(clienttimeoutOption);
		options.addOption(maxinlineOption);
		options.addOption(sendQueueOption);
		options.addOption(recvQueueOption);
		options.addOption(serializedSizeOption);
		CommandLineParser parser = new DefaultParser();
		
		try {
			CommandLine line = parser.parse(options, args);
			host = line.getOptionValue(addressOption.getOpt());

			if (line.hasOption(loopOption.getOpt())) {
				loop = Integer.parseInt(line.getOptionValue(loopOption.getOpt()));
			}
			if (line.hasOption(threadCountOption.getOpt())) {
				threadCount = Integer.parseInt(line.getOptionValue(threadCountOption.getOpt()));
			}
			if (line.hasOption(modeOption.getOpt())) {
				String _mode = line.getOptionValue(modeOption.getOpt());
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
			}
			if (line.hasOption(batchSizeOption.getOpt())) {
				batchSize = Integer.parseInt(line.getOptionValue(batchSizeOption.getOpt()));
			}
			if (line.hasOption(connectionsOption.getOpt())) {
				connections = Integer.parseInt(line.getOptionValue(connectionsOption.getOpt()));
			}
			if (line.hasOption(clienttimeoutOption.getOpt())) {
				clienttimeout = Integer.parseInt(line.getOptionValue(clienttimeoutOption.getOpt()));
			}
			if (line.hasOption(maxinlineOption.getOpt())) {
				maxinline = Integer.parseInt(line.getOptionValue(maxinlineOption.getOpt()));
			}
			if (line.hasOption(recvQueueOption.getOpt())) {
				recvQueue = Integer.parseInt(line.getOptionValue(recvQueueOption.getOpt()));
			}
			if (line.hasOption(sendQueueOption.getOpt())) {
				sendQueue = Integer.parseInt(line.getOptionValue(sendQueueOption.getOpt()));
			}
			if (line.hasOption(serializedSizeOption.getOpt())) {
				RdmaRpcRequest.SERIALIZED_SIZE = Integer.parseInt(line.getOptionValue(serializedSizeOption.getOpt()));
				RdmaRpcResponse.SERIALIZED_SIZE = RdmaRpcRequest.SERIALIZED_SIZE;
			}
		} catch (ParseException e) {
			HelpFormatter formatter = new HelpFormatter();
			formatter.printHelp("DaRPCClient", options);
			System.exit(-1); // undefined option
		}

		if ((threadCount % connections) != 0){
			throw new Exception("thread count needs to be a multiple of connections");
		}
		
		int threadsperconnection = threadCount / connections;
		DaRPCEndpoint<?,?>[] rpcConnections = new DaRPCEndpoint[connections];
		Thread[] workers = new Thread[threadCount];
		ClientThread[] benchmarkTask = new ClientThread[threadCount];
		
		RdmaRpcProtocol rpcProtocol = new RdmaRpcProtocol();
		System.out.println("starting.. threads " + threadCount + ", connections " + connections + ", server " + host + ", recvQueue " + recvQueue + ", sendQueue" + sendQueue + ", batchSize " + batchSize + ", mode " + mode);
		DaRPCClientGroup<RdmaRpcRequest, RdmaRpcResponse> group = DaRPCClientGroup.createClientGroup(rpcProtocol, 100, maxinline, recvQueue, sendQueue);
		InetSocketAddress address = new InetSocketAddress(host, 1919);
		
		int k = 0;
		for (int i = 0; i < rpcConnections.length; i++){
			DaRPCClientEndpoint<RdmaRpcRequest, RdmaRpcResponse> clientEp = group.createEndpoint();
			clientEp.connect(address, 1000);
			rpcConnections[i] = clientEp;
			for (int j = 0; j < threadsperconnection; j++){
				benchmarkTask[k] = new ClientThread(clientEp, loop, address, mode, batchSize, clienttimeout);
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
