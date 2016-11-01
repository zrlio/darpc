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
import com.ibm.disni.rdma.*;


public abstract class RpcEndpointGroup<E extends RpcEndpoint<R,T>, R extends RpcMessage, T extends RpcMessage> extends RdmaEndpointGroup<E> {
	private static final Logger logger = LoggerFactory.getLogger("com.ibm.darpc");
	private static int DARPC_VERSION = 46;
	
	private int cqSize;
	private int rpcpipeline;
	private int maxSge;
	private int timeout;
	private int bufferSize;
	private int maxInline;
	
	public static int getVersion(){
		return DARPC_VERSION;
	}	
	
	protected RpcEndpointGroup(RpcProtocol<R,T> protocol, int timeout, int maxinline, int rpcpipeline, int maxSge, int cqSize) throws Exception {
		super(timeout);
		this.timeout = timeout;
		this.bufferSize = Math.max(protocol.createRequest().size(), protocol.createResponse().size());
		this.rpcpipeline = rpcpipeline;
		this.maxSge = maxSge;
		this.cqSize = cqSize;
		this.maxInline = maxinline;
//		logger.info("rpc group, rpcpipeline " + rpcpipeline + ", effictive poolsize " + computeAffinities.length + ", maxinline " + maxInline + ", version 1");
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
	
	public int getTimeout() {
		return timeout;
	}
	
	public int getBufferSize() {
		return bufferSize;
	}	

	public void close() throws IOException, InterruptedException {
		super.close();
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
