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

public class DaRPCServerGroup<R extends DaRPCMessage, T extends DaRPCMessage> extends DaRPCEndpointGroup<DaRPCServerEndpoint<R,T>, R, T> {
	private static final Logger logger = LoggerFactory.getLogger("com.ibm.darpc");

	private ConcurrentHashMap<Integer, DaRPCInstance<R,T>> deviceInstance;
	private DaRPCResourceManager resourceManager;
	private long[] computeAffinities;
	private long[] resourceAffinities;
	private int currentCluster;
	private int nbrOfClusters;
	private DaRPCService<R, T> rpcService;
	private boolean polling;
	private int pollSize;
	private int clusterSize;

	public static <R extends DaRPCMessage, T extends DaRPCMessage> DaRPCServerGroup<R, T> createServerGroup(DaRPCService<R, T> rpcService, DaRPCMemPool<DaRPCServerEndpoint<R,T>, R, T> memPool, long[] clusterAffinities, int timeout, int maxinline, boolean polling,
																											int recvQueue, int sendQueue, int pollSize, int clusterSize) throws Exception {
		DaRPCServerGroup<R,T> group = new DaRPCServerGroup<R,T>(rpcService, memPool, clusterAffinities, timeout, maxinline, polling,
																recvQueue, sendQueue, pollSize, clusterSize);
		group.init(new RpcServerFactory<R,T>(group));
		return group;
	}

	private DaRPCServerGroup(DaRPCService<R, T> rpcService, DaRPCMemPool<DaRPCServerEndpoint<R,T>, R, T> memPool,long[] clusterAffinities, int timeout, int maxinline,
							 boolean polling, int recvQueue, int sendQueue, int pollSize, int clusterSize) throws Exception {
		super(rpcService, memPool, timeout, maxinline, recvQueue, sendQueue);

		this.rpcService = rpcService;
		deviceInstance = new ConcurrentHashMap<Integer, DaRPCInstance<R,T>>();
		this.computeAffinities = clusterAffinities;
		this.resourceAffinities = clusterAffinities;
		this.nbrOfClusters = computeAffinities.length;
		this.currentCluster = 0;
		resourceManager = new DaRPCResourceManager(resourceAffinities, timeout);
		this.polling = polling;
		this.pollSize = pollSize;
		this.clusterSize = clusterSize;
	}

	public RdmaCqProvider createCqProvider(DaRPCServerEndpoint<R,T> endpoint) throws IOException {
		logger.info("setting up cq processor (multicore)");
		IbvContext context = endpoint.getIdPriv().getVerbs();
		if (context == null) {
			throw new IOException("setting up cq processor, no context found");
		}
		DaRPCInstance<R,T> rpcInstance = null;
		int key = context.getCmd_fd();
		if (!deviceInstance.containsKey(key)) {
			int cqSize = (this.recvQueueSize() + this.sendQueueSize())*clusterSize;
			rpcInstance = new DaRPCInstance<R,T>(context, cqSize, this.pollSize, computeAffinities, this.getTimeout(), polling);
			deviceInstance.put(context.getCmd_fd(), rpcInstance);
		}
		rpcInstance = deviceInstance.get(context.getCmd_fd());
		DaRPCCluster<R,T> cqProcessor = rpcInstance.getProcessor(endpoint.clusterId());
		return cqProcessor;
	}

	public IbvQP createQpProvider(DaRPCServerEndpoint<R,T> endpoint) throws IOException{
		logger.info("setting up QP");
		DaRPCCluster<R,T>  cqProcessor = this.lookupCqProcessor(endpoint);
		IbvCQ cq = cqProcessor.getCQ();
		IbvQP qp = this.createQP(endpoint.getIdPriv(), endpoint.getPd(), cq);
		cqProcessor.registerQP(qp.getQp_num(), endpoint);
		return qp;
	}

	public void allocateResources(DaRPCServerEndpoint<R,T> endpoint) throws Exception {
		resourceManager.allocateResources(endpoint);
	}

	synchronized int newClusterId() {
		int newClusterId = currentCluster;
		currentCluster = (currentCluster + 1) % nbrOfClusters;
		return newClusterId;
	}

	protected synchronized DaRPCCluster<R,T> lookupCqProcessor(DaRPCServerEndpoint<R,T> endpoint) throws IOException{
		IbvContext context = endpoint.getIdPriv().getVerbs();
		if (context == null) {
			throw new IOException("setting up cq processor, no context found");
		}
		DaRPCInstance<R,T> rpcInstance = null;
		int key = context.getCmd_fd();
		if (!deviceInstance.containsKey(key)) {
			return null;
		} else {
			rpcInstance = deviceInstance.get(context.getCmd_fd());
			DaRPCCluster<R,T> cqProcessor = rpcInstance.getProcessor(endpoint.clusterId());
			return cqProcessor;
		}
	}

	public void close() throws IOException, InterruptedException {
		super.close();
		for (DaRPCInstance<R,T> rpcInstance : deviceInstance.values()){
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

	public void processServerEvent(DaRPCServerEvent<R,T> event) throws IOException {
		rpcService.processServerEvent(event);
	}

	public void open(DaRPCServerEndpoint<R,T> endpoint){
		rpcService.open(endpoint);
	}

	public void close(DaRPCServerEndpoint<R,T> endpoint){
		rpcService.close(endpoint);
	}

	public DaRPCService<? extends DaRPCMessage, ? extends DaRPCMessage> getRpcService() {
		return rpcService;
	}

	public static class RpcServerFactory<R extends DaRPCMessage, T extends DaRPCMessage> implements RdmaEndpointFactory<DaRPCServerEndpoint<R,T>> {
		private DaRPCServerGroup<R, T> group;

		public RpcServerFactory(DaRPCServerGroup<R, T> group){
			this.group = group;
		}

		@Override
		public DaRPCServerEndpoint<R,T> createEndpoint(RdmaCmId id, boolean serverSide) throws IOException {
			return new DaRPCServerEndpoint<R,T>(group, id, serverSide);
		}
	}
}
