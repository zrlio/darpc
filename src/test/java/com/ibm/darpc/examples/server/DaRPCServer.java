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

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.URI;

import com.ibm.darpc.DaRPCServerEndpoint;
import com.ibm.darpc.DaRPCServerGroup;
import com.ibm.darpc.examples.protocol.RdmaRpcRequest;
import com.ibm.darpc.examples.protocol.RdmaRpcResponse;
import com.ibm.disni.rdma.*;
import com.ibm.disni.util.*;

public class DaRPCServer {
	private String ipAddress; 
	private int poolsize = 3;
	private int recvQueue = 16;
	private int sendQueue = 16;
	private int wqSize = recvQueue;
	private int servicetimeout = 0;
	private boolean polling = false;
	private int maxinline = 0;
	private int connections = 16;
	
	public void run() throws Exception{
		long[] clusterAffinities = new long[poolsize];
		for (int i = 0; i < poolsize; i++){
			long cpu = 1L << i;
			clusterAffinities[i] = cpu;
		}
		System.out.println("running...server " + ipAddress + ", poolsize " + poolsize + ", maxinline " + maxinline + ", polling " + polling + ", recvQueue " + recvQueue + ", sendQueue " + sendQueue + ", wqSize " + wqSize + ", rpcservice-timeout " + servicetimeout);
		RdmaRpcService rpcService = new RdmaRpcService(servicetimeout);
		DaRPCServerGroup<RdmaRpcRequest, RdmaRpcResponse> group = DaRPCServerGroup.createServerGroup(rpcService, clusterAffinities, -1, maxinline, polling, recvQueue, sendQueue, wqSize, 32); 
		RdmaServerEndpoint<DaRPCServerEndpoint<RdmaRpcRequest, RdmaRpcResponse>> serverEp = group.createServerEndpoint();
		URI uri = URI.create("rdma://" + ipAddress + ":" + 1919);
		serverEp.bind(uri);
		while(true){
			serverEp.accept();
		}		
	}
	
	public void launch(String[] args) throws Exception {
		String[] _args = args;
		if (args.length < 1) {
			System.exit(0);
		} else if (args[0].equals(DaRPCServer.class.getCanonicalName())) {
			_args = new String[args.length - 1];
			for (int i = 0; i < _args.length; i++) {
				_args[i] = args[i + 1];
			}
		}

		GetOpt go = new GetOpt(_args, "a:p:s:da:c:r:s:w:l:");
		go.optErr = true;
		int ch = -1;

		while ((ch = go.getopt()) != GetOpt.optEOF) {
			if ((char) ch == 'a') {
				ipAddress = go.optArgGet();
			} else if ((char) ch == 'p') {
				poolsize = Integer.parseInt(go.optArgGet());
			} else if ((char) ch == 't') {
				servicetimeout = Integer.parseInt(go.optArgGet());
			} else if ((char) ch == 'd') {
				polling = true;
			} else if ((char) ch == 'i') {
				maxinline = Integer.parseInt(go.optArgGet());
			} else if ((char) ch == 'c') {
				connections = Integer.parseInt(go.optArgGet());
			} else if ((char) ch == 'w') {
				wqSize = Integer.parseInt(go.optArgGet());
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
		
		this.run();
	}
	
	public static void main(String[] args) throws Exception { 
		DaRPCServer rpcServer = new DaRPCServer();
		rpcServer.launch(args);		
	}	
}
