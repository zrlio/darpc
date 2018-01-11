package com.ibm.darpc;

import java.io.IOException;

import com.ibm.disni.rdma.RdmaCqProvider;
import com.ibm.disni.rdma.RdmaEndpointFactory;
import com.ibm.disni.rdma.verbs.IbvCQ;
import com.ibm.disni.rdma.verbs.IbvQP;
import com.ibm.disni.rdma.verbs.RdmaCmId;

public class DaRPCClientGroup<R extends DaRPCMessage, T extends DaRPCMessage> extends DaRPCEndpointGroup<DaRPCClientEndpoint<R,T>, R, T> {
	public static <R extends DaRPCMessage, T extends DaRPCMessage> DaRPCClientGroup<R, T> createClientGroup(DaRPCProtocol<R, T> protocol, DaRPCMemPool<DaRPCClientEndpoint<R,T>, R, T> memPool, int timeout, int maxinline, int recvQueue, int sendQueue) throws Exception {
		DaRPCClientGroup<R,T> group = new DaRPCClientGroup<R,T>(protocol, memPool, timeout, maxinline, recvQueue, sendQueue);
		group.init(new RpcClientFactory<R,T>(group));
		return group;
	}

	private DaRPCClientGroup(DaRPCProtocol<R, T> protocol, DaRPCMemPool<DaRPCClientEndpoint<R,T>, R, T> memPool, int timeout, int maxinline, int recvQueue, int sendQueue)
			throws Exception {
		super(protocol, memPool, timeout, maxinline, recvQueue, sendQueue);
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
