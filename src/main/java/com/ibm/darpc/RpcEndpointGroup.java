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
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ibm.disni.rdma.verbs.*;
import com.ibm.disni.rdma.*;


public abstract class RpcEndpointGroup<R extends RdmaRpcMessage, T extends RdmaRpcMessage> extends RdmaEndpointGroup<RpcEndpoint<R,T>> {
	private static final Logger logger = LoggerFactory.getLogger("com.ibm.darpc");
	private static int DARPC_VERSION = 46;
	
	protected int maxInline;
	protected ConcurrentHashMap<Integer, RpcInstance<R,T>> deviceInstance;
	protected RpcResourceManager resourceManager;
	protected long[] computeAffinities;
	protected long[] resourceAffinities;
	protected int currentCluster;
	protected int nbrOfClusters;
	protected int cqSize;
	protected int rpcpipeline;
	protected int maxSge;
	protected int timeout;
	protected boolean polling;
	protected RdmaRpcService<R, T> rpcService;
	protected int bufferSize;
	
	public static int getVersion(){
		return DARPC_VERSION;
	}	
	
	protected RpcEndpointGroup(RdmaRpcService<R, T> rpcService, long clusterAffinities[], int timeout, int maxinline, boolean polling, int rpcpipeline, int maxSge, int cqSize) throws Exception {
		super(timeout);
		deviceInstance = new ConcurrentHashMap<Integer, RpcInstance<R,T>>();
		this.timeout = timeout;
		this.bufferSize = Math.max(rpcService.createRequest().size(), rpcService.createResponse().size());
		this.computeAffinities = clusterAffinities;
		this.resourceAffinities = clusterAffinities;		
		this.nbrOfClusters = computeAffinities.length;
		this.rpcpipeline = rpcpipeline;
		this.maxSge = maxSge;
		this.cqSize = cqSize;
		this.currentCluster = 0;
		resourceManager = new RpcResourceManager(resourceAffinities, timeout);
		this.rpcService = rpcService;
		this.maxInline = maxinline;
		this.polling = polling;
//		logger.info("rpc group, rpcpipeline " + rpcpipeline + ", effictive poolsize " + computeAffinities.length + ", maxinline " + maxInline + ", version 1");
	}	
	
	public R createRequest() {
		return rpcService.createRequest();
	}

	public T createResponse() {
		return rpcService.createResponse();
	}
	
	public void processServerEvent(RpcServerEvent<R,T> event) throws IOException {
		rpcService.processServerEvent(event);
	}
	
	public void close(RpcEndpoint<R,T> endpoint){
		rpcService.close(endpoint);
	}
	
	public void allocateResources(RpcEndpoint<R,T> endpoint) throws Exception {
		resourceManager.allocateResources(endpoint);
	}
	
	synchronized int newClusterId() {
		int newClusterId = currentCluster;
		currentCluster = (currentCluster + 1) % nbrOfClusters;
		return newClusterId;
	}
	
	protected synchronized RpcCluster<R,T> createCqProcessor(RpcEndpoint<R,T> endpoint) throws IOException{
		IbvContext context = endpoint.getIdPriv().getVerbs();
		if (context == null) {
			throw new IOException("setting up cq processor, no context found");
		}
		
		logger.info("setting up cq pool, context found");
		RpcInstance<R,T> rpcInstance = null;
		int key = context.getCmd_fd();
		if (!deviceInstance.containsKey(key)) {
			rpcInstance = new RpcInstance<R,T>(context, cqSize, rpcpipeline, computeAffinities, timeout, polling);
			deviceInstance.put(context.getCmd_fd(), rpcInstance);
		}
		rpcInstance = deviceInstance.get(context.getCmd_fd());
		RpcCluster<R,T> cqProcessor = rpcInstance.getProcessor(endpoint.clusterId());
		return cqProcessor;
	}
	
	protected synchronized RpcCluster<R,T> lookupCqProcessor(RpcEndpoint<R,T> endpoint) throws IOException{
		IbvContext context = endpoint.getIdPriv().getVerbs();
		if (context == null) {
			throw new IOException("setting up cq processor, no context found");
		}
		
//		logger.info("setting up cq pool, context found");
		RpcInstance<R,T> rpcInstance = null;
		int key = context.getCmd_fd();
		if (!deviceInstance.containsKey(key)) {
			return null;
		} else {
			rpcInstance = deviceInstance.get(context.getCmd_fd());
			RpcCluster<R,T> cqProcessor = rpcInstance.getProcessor(endpoint.clusterId());
			return cqProcessor;
		}
	}	
	
	protected synchronized IbvQP createQP(RdmaCmId id, IbvPd pd, IbvCQ cq) throws IOException{
		IbvQPInitAttr attr = new IbvQPInitAttr();
		attr.cap().setMax_recv_sge(maxSge);
		attr.cap().setMax_recv_wr(rpcpipeline);
		attr.cap().setMax_send_sge(maxSge);
		attr.cap().setMax_send_wr(rpcpipeline);
		attr.cap().setMax_inline_data(maxInline);
		attr.setQp_type(IbvQP.IBV_QPT_RC);
		attr.setRecv_cq(cq);
		attr.setSend_cq(cq);		
		IbvQP qp = id.createQP(pd, attr);
		return qp;
	}
	
	public RdmaRpcService<? extends RdmaRpcMessage, ? extends RdmaRpcMessage> getRpcService() {
		return rpcService;
	}

	public int getTimeout() {
		return timeout;
	}
	
	public int getBufferSize() {
		return bufferSize;
	}	

	public void close() throws IOException, InterruptedException {
		super.close();
		for (RpcInstance<R,T> rpcInstance : deviceInstance.values()){
			rpcInstance.close();
		}		
		resourceManager.close();
		logger.info("rpc group down");
	}	
	
	public int getRpcpipeline() {
		return rpcpipeline;
	}	
	
	public int getMaxInline() {
		return maxInline;
	}

	public int getCqSize() {
		return cqSize;
	}

	public int getMaxSge() {
		return maxSge;
	}
}
