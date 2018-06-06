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

package com.ibm.darpc.examples.server;

import java.net.InetSocketAddress;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import com.ibm.darpc.DaRPCServerEndpoint;
import com.ibm.darpc.DaRPCServerGroup;
import com.ibm.darpc.examples.protocol.RdmaRpcRequest;
import com.ibm.darpc.examples.protocol.RdmaRpcResponse;
import com.ibm.disni.rdma.*;

public class DaRPCServer {
	private String host; 
	private int poolsize = 3;
	private int recvQueue = 16;
	private int sendQueue = 16;
	private int wqSize = recvQueue;
	private int servicetimeout = 0;
	private boolean polling = false;
	private int maxinline = 0;
//	private int connections = 16;
	
	public void run() throws Exception{
		long[] clusterAffinities = new long[poolsize];
		for (int i = 0; i < poolsize; i++){
			long cpu = 1L << i;
			clusterAffinities[i] = cpu;
		}
		System.out.println("running...server " + host + ", poolsize " + poolsize + ", maxinline " + maxinline + ", polling " + polling + ", recvQueue " + recvQueue + ", sendQueue " + sendQueue + ", wqSize " + wqSize + ", rpcservice-timeout " + servicetimeout);
		RdmaRpcService rpcService = new RdmaRpcService(servicetimeout);
		DaRPCServerGroup<RdmaRpcRequest, RdmaRpcResponse> group = DaRPCServerGroup.createServerGroup(rpcService, clusterAffinities, -1, maxinline, polling, recvQueue, sendQueue, wqSize, 32); 
		RdmaServerEndpoint<DaRPCServerEndpoint<RdmaRpcRequest, RdmaRpcResponse>> serverEp = group.createServerEndpoint();
		InetSocketAddress address = new InetSocketAddress(host, 1919);
		serverEp.bind(address, 100);
		while(true){
			serverEp.accept();
		}		
	}
	
	public void launch(String[] args) throws Exception {
		Option addressOption = Option.builder("a").required().desc("server address").hasArg().build();
		Option poolsizeOption = Option.builder("p").desc("pool size").hasArg().build();
		Option servicetimeoutOption = Option.builder("t").desc("service timeout").hasArg().build();
		Option pollingOption = Option.builder("d").desc("if polling, default false").build();
		Option maxinlineOption = Option.builder("i").desc("max inline data").hasArg().build();
		Option wqSizeOption = Option.builder("w").desc("wq size").hasArg().build();
		Option recvQueueOption = Option.builder("r").desc("receive queue").hasArg().build();
		Option sendQueueOption = Option.builder("s").desc("send queue").hasArg().build();
		Option serializedSizeOption = Option.builder("l").desc("serialized size").hasArg().build();
		Options options = new Options();
		options.addOption(addressOption);
		options.addOption(poolsizeOption);
		options.addOption(servicetimeoutOption);
		options.addOption(pollingOption);
		options.addOption(maxinlineOption);
		options.addOption(wqSizeOption);
		options.addOption(recvQueueOption);
		options.addOption(sendQueueOption);
		options.addOption(serializedSizeOption);
		CommandLineParser parser = new DefaultParser();

		try {
			CommandLine line = parser.parse(options, args);
			host = line.getOptionValue(addressOption.getOpt());

			if (line.hasOption(poolsizeOption.getOpt())) {
				poolsize = Integer.parseInt(line.getOptionValue(poolsizeOption.getOpt()));
			}
			if (line.hasOption(servicetimeoutOption.getOpt())) {
				servicetimeout = Integer.parseInt(line.getOptionValue(servicetimeoutOption.getOpt()));
			}
			if (line.hasOption(pollingOption.getOpt())) {
				polling = true;
			}
			if (line.hasOption(maxinlineOption.getOpt())) {
				maxinline = Integer.parseInt(line.getOptionValue(maxinlineOption.getOpt()));
			}
			if (line.hasOption(wqSizeOption.getOpt())) {
				wqSize = Integer.parseInt(line.getOptionValue(wqSizeOption.getOpt()));
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
			formatter.printHelp("DaRPCServer", options);
			System.exit(-1);
		}
		this.run();
	}
	
	public static void main(String[] args) throws Exception { 
		DaRPCServer rpcServer = new DaRPCServer();
		rpcServer.launch(args);		
	}	
}
