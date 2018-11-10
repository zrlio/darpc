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

package com.ibm.darpc;

import java.io.IOException;

import com.ibm.disni.RdmaCqProvider;
import com.ibm.disni.RdmaEndpointFactory;
import com.ibm.disni.verbs.IbvCQ;
import com.ibm.disni.verbs.IbvQP;
import com.ibm.disni.verbs.RdmaCmId;

public class DaRPCClientGroup<R extends DaRPCMessage, T extends DaRPCMessage> extends DaRPCEndpointGroup<DaRPCClientEndpoint<R,T>, R, T> {
	public static <R extends DaRPCMessage, T extends DaRPCMessage> DaRPCClientGroup<R, T> createClientGroup(DaRPCProtocol<R, T> protocol, int timeout, int maxinline, int recvQueue, int sendQueue) throws Exception {
		DaRPCClientGroup<R,T> group = new DaRPCClientGroup<R,T>(protocol, timeout, maxinline, recvQueue, sendQueue);
		group.init(new RpcClientFactory<R,T>(group));
		return group;
	}	
	
	private DaRPCClientGroup(DaRPCProtocol<R, T> protocol, int timeout, int maxinline, int recvQueue, int sendQueue)
			throws Exception {
		super(protocol, timeout, maxinline, recvQueue, sendQueue);
	}
	

	@Override
	public void allocateResources(DaRPCClientEndpoint<R, T> endpoint) throws Exception {
		endpoint.allocateResources();
	}

	@Override
	public RdmaCqProvider createCqProvider(DaRPCClientEndpoint<R, T> endpoint) throws IOException {
		return new RdmaCqProvider(endpoint.getIdPriv().getVerbs(), recvQueueSize() + sendQueueSize());
	}

	@Override
	public IbvQP createQpProvider(DaRPCClientEndpoint<R, T> endpoint) throws IOException {
		RdmaCqProvider cqProvider = endpoint.getCqProvider();
		IbvCQ cq = cqProvider.getCQ();
		IbvQP qp = this.createQP(endpoint.getIdPriv(), endpoint.getPd(), cq);
		return qp;
	}
	
	public static class RpcClientFactory<R extends DaRPCMessage, T extends DaRPCMessage> implements RdmaEndpointFactory<DaRPCClientEndpoint<R,T>> {
		private DaRPCClientGroup<R, T> group;
		
		public RpcClientFactory(DaRPCClientGroup<R, T> group){
			this.group = group;
		}
		
		@Override
		public DaRPCClientEndpoint<R,T> createEndpoint(RdmaCmId id, boolean serverSide) throws IOException {
			return new DaRPCClientEndpoint<R,T>(group, id, serverSide);
		}
	}	

}
