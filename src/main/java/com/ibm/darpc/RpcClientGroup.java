package com.ibm.darpc;

import java.io.IOException;

import com.ibm.disni.rdma.RdmaCqProvider;
import com.ibm.disni.rdma.RdmaEndpointFactory;
import com.ibm.disni.rdma.verbs.IbvCQ;
import com.ibm.disni.rdma.verbs.IbvQP;
import com.ibm.disni.rdma.verbs.RdmaCmId;

public class RpcClientGroup<R extends RpcMessage, T extends RpcMessage> extends RpcEndpointGroup<RpcClientEndpoint<R,T>, R, T> {
	public static <R extends RpcMessage, T extends RpcMessage> RpcClientGroup<R, T> createClientGroup(RpcProtocol<R, T> protocol, int timeout, int maxinline, int rpcpipeline) throws Exception {
		RpcClientGroup<R,T> group = new RpcClientGroup<R,T>(protocol, timeout, maxinline, rpcpipeline, rpcpipeline*2);
		group.init(new RpcClientFactory<R,T>(group));
		return group;
	}	
	
	private RpcClientGroup(RpcProtocol<R, T> protocol, int timeout, int maxinline, int rpcpipeline, int cqSize)
			throws Exception {
		super(protocol, timeout, maxinline, rpcpipeline, cqSize);
	}
	

	@Override
	public void allocateResources(RpcClientEndpoint<R, T> endpoint) throws Exception {
		endpoint.allocateResources();
	}

	@Override
	public RdmaCqProvider createCqProvider(RpcClientEndpoint<R, T> endpoint) throws IOException {
		return new RdmaCqProvider(endpoint.getIdPriv().getVerbs(), this.getCqSize());
	}

	@Override
	public IbvQP createQpProvider(RpcClientEndpoint<R, T> endpoint) throws IOException {
		RdmaCqProvider cqProvider = endpoint.getCqProvider();
		IbvCQ cq = cqProvider.getCQ();
		IbvQP qp = this.createQP(endpoint.getIdPriv(), endpoint.getPd(), cq);
		return qp;
	}
	
	public static class RpcClientFactory<R extends RpcMessage, T extends RpcMessage> implements RdmaEndpointFactory<RpcClientEndpoint<R,T>> {
		private RpcClientGroup<R, T> group;
		
		public RpcClientFactory(RpcClientGroup<R, T> group){
			this.group = group;
		}
		
		@Override
		public RpcClientEndpoint<R,T> createEndpoint(RdmaCmId id, boolean serverSide) throws IOException {
			return new RpcClientEndpoint<R,T>(group, id, serverSide);
		}
	}	

}
