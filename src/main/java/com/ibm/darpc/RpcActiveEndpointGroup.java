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

package com.ibm.darpc;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ibm.disni.rdma.verbs.*;
import com.ibm.disni.rdma.endpoints.*;

public class RpcActiveEndpointGroup<R extends RdmaRpcMessage, T extends RdmaRpcMessage> extends RpcEndpointGroup<R,T> {
	private static final Logger logger = LoggerFactory.getLogger("com.ibm.darpc");
	
	public static <R extends RdmaRpcMessage, T extends RdmaRpcMessage> RpcActiveEndpointGroup<R,T> createDefault(RdmaRpcService<R, T> rpcService, long clusterAffinities[], int timeout, int maxinline, boolean polling, int rpcpipeline, int maxSge, int cqSize) throws Exception {
		RpcActiveEndpointGroup<R,T> group = new RpcActiveEndpointGroup<R,T>(rpcService, clusterAffinities, timeout, maxinline, polling, rpcpipeline, maxSge, cqSize);
		RpcEndpointFactory<R,T> factory = new RpcEndpointFactory<R,T>(group);
		group.init(factory);
		return group;
	}
	
	protected RpcActiveEndpointGroup(RdmaRpcService<R, T> rpcService, long clusterAffinities[], int timeout, int maxinline, boolean polling, int rpcpipeline, int maxSge, int cqSize) throws Exception {
		super(rpcService, clusterAffinities, timeout, maxinline, polling, rpcpipeline, maxSge, cqSize);
		logger.info("rpc active endpoint group (2), maxWR " + rpcpipeline + ", maxSge " + maxSge + ", cqSize " + cqSize);
	}	
	
	//------------
	
	public RdmaCqProvider createCqProvider(RpcClientEndpoint<R,T> endpoint)
			throws IOException {
		logger.info("setting up cq processor (multicore)");
		return createCqProcessor(endpoint);
	}	
	
	public IbvQP createQpProvider(RpcClientEndpoint<R,T> endpoint) throws IOException{
		RpcCluster<R,T>  cqProcessor = this.lookupCqProcessor(endpoint);
		IbvCQ cq = cqProcessor.getCQ();
		IbvQP qp = this.createQP(endpoint.getIdPriv(), endpoint.getPd(), cq);
		logger.info("registering endpoint with cq");
		cqProcessor.registerQP(qp.getQp_num(), endpoint);
		return qp;
	}	
	
	public static class RpcEndpointFactory<R extends RdmaRpcMessage, T extends RdmaRpcMessage> implements RdmaEndpointFactory<RpcClientEndpoint<R,T>> {
		private RpcEndpointGroup<R, T> group;
		
		public RpcEndpointFactory(RpcEndpointGroup<R, T> group){
			this.group = group;
		}
		
		@Override
		public RpcClientEndpoint<R,T> createClientEndpoint(RdmaCmId id) throws IOException {
			return new RpcActiveClientEndpoint<R,T>(group, id);
		}
	}	
}
