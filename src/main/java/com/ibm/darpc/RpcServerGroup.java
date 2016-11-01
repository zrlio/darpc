package com.ibm.darpc;

import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ibm.disni.rdma.RdmaCqProvider;
import com.ibm.disni.rdma.RdmaEndpointFactory;
import com.ibm.disni.rdma.verbs.IbvCQ;
import com.ibm.disni.rdma.verbs.IbvContext;
import com.ibm.disni.rdma.verbs.IbvQP;
import com.ibm.disni.rdma.verbs.RdmaCmId;

public class RpcServerGroup<R extends RpcMessage, T extends RpcMessage> extends RpcEndpointGroup<RpcServerEndpoint<R,T>, R, T> {
	private static final Logger logger = LoggerFactory.getLogger("com.ibm.darpc");
	
	private ConcurrentHashMap<Integer, RpcInstance<R,T>> deviceInstance;
	private RpcResourceManager resourceManager;
	private long[] computeAffinities;
	private long[] resourceAffinities;
	private int currentCluster;
	private int nbrOfClusters;
	private RpcService<R, T> rpcService;
	private boolean polling;
	
	public static <R extends RpcMessage, T extends RpcMessage> RpcServerGroup<R, T> createServerGroup(RpcService<R, T> rpcService, long[] clusterAffinities, int timeout, int maxinline, boolean polling, int rpcpipeline, int maxSge, int cqSize) throws Exception {
		RpcServerGroup<R,T> group = new RpcServerGroup<R,T>(rpcService, clusterAffinities, timeout, maxinline, polling, rpcpipeline, maxSge, cqSize);
		group.init(null);
		return group;
	}

	private RpcServerGroup(RpcService<R, T> rpcService, long[] clusterAffinities, int timeout, int maxinline, boolean polling, int rpcpipeline, int maxSge, int cqSize) throws Exception {
		super(rpcService, timeout, maxinline, rpcpipeline, maxSge, cqSize);
		
		this.rpcService = rpcService;
		deviceInstance = new ConcurrentHashMap<Integer, RpcInstance<R,T>>();
		this.computeAffinities = clusterAffinities;
		this.resourceAffinities = clusterAffinities;		
		this.nbrOfClusters = computeAffinities.length;
		this.currentCluster = 0;
		resourceManager = new RpcResourceManager(resourceAffinities, timeout);
		this.polling = polling;		
	}
	
	public RdmaCqProvider createCqProvider(RpcServerEndpoint<R,T> endpoint) throws IOException {
		logger.info("setting up cq processor (multicore)");
		return createCqProcessor(endpoint);
	}	
	
	public IbvQP createQpProvider(RpcServerEndpoint<R,T> endpoint) throws IOException{
		RpcCluster<R,T>  cqProcessor = this.lookupCqProcessor(endpoint);
		IbvCQ cq = cqProcessor.getCQ();
		IbvQP qp = this.createQP(endpoint.getIdPriv(), endpoint.getPd(), cq);	
		logger.info("registering endpoint with cq");
		cqProcessor.registerQP(qp.getQp_num(), endpoint);
		return qp;
	}		
	
	public void allocateResources(RpcServerEndpoint<R,T> endpoint) throws Exception {
		resourceManager.allocateResources(endpoint);
	}
	
	synchronized int newClusterId() {
		int newClusterId = currentCluster;
		currentCluster = (currentCluster + 1) % nbrOfClusters;
		return newClusterId;
	}
	
	protected synchronized RpcCluster<R,T> createCqProcessor(RpcServerEndpoint<R,T> endpoint) throws IOException{
		IbvContext context = endpoint.getIdPriv().getVerbs();
		if (context == null) {
			throw new IOException("setting up cq processor, no context found");
		}
		
		logger.info("setting up cq pool, context found");
		RpcInstance<R,T> rpcInstance = null;
		int key = context.getCmd_fd();
		if (!deviceInstance.containsKey(key)) {
			rpcInstance = new RpcInstance<R,T>(context, this.getCqSize(), this.getRpcpipeline(), computeAffinities, this.getTimeout(), polling);
			deviceInstance.put(context.getCmd_fd(), rpcInstance);
		}
		rpcInstance = deviceInstance.get(context.getCmd_fd());
		RpcCluster<R,T> cqProcessor = rpcInstance.getProcessor(endpoint.clusterId());
		return cqProcessor;
	}
	
	protected synchronized RpcCluster<R,T> lookupCqProcessor(RpcServerEndpoint<R,T> endpoint) throws IOException{
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
	
	public void close() throws IOException, InterruptedException {
		super.close();
		for (RpcInstance<R,T> rpcInstance : deviceInstance.values()){
			rpcInstance.close();
		}		
		resourceManager.close();
		logger.info("rpc group down");
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
	
	public void close(RpcServerEndpoint<R,T> endpoint){
		rpcService.close(endpoint);
	}
	
	public RpcService<? extends RpcMessage, ? extends RpcMessage> getRpcService() {
		return rpcService;
	}
	
	public static class RpcServerFactory<R extends RpcMessage, T extends RpcMessage> implements RdmaEndpointFactory<RpcServerEndpoint<R,T>> {
		private RpcServerGroup<R, T> group;
		
		public RpcServerFactory(RpcServerGroup<R, T> group){
			this.group = group;
		}
		
		@Override
		public RpcServerEndpoint<R,T> createEndpoint(RdmaCmId id, boolean serverSide) throws IOException {
			return new RpcServerEndpoint<R,T>(group, id, serverSide);
		}
	}	
}
