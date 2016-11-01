package com.ibm.darpc;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.ArrayBlockingQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ibm.disni.rdma.verbs.IbvWC;
import com.ibm.disni.rdma.verbs.RdmaCmEvent;
import com.ibm.disni.rdma.verbs.RdmaCmId;
import com.ibm.disni.rdma.verbs.SVCPostRecv;

public class RpcServerEndpoint<R extends RpcMessage, T extends RpcMessage> extends RpcEndpoint<R,T> {
	private static final Logger logger = LoggerFactory.getLogger("com.ibm.darpc");
	
	private RpcServerGroup<R, T> group;
	private ArrayBlockingQueue<RpcServerEvent<R,T>> eventPool;
	private ArrayBlockingQueue<RpcServerEvent<R,T>> pendingEvents;
	private ArrayBlockingQueue<SendOperation> freePostSend;
	private int getClusterId;
	
	public RpcServerEndpoint(RpcServerGroup<R, T> endpointGroup, RdmaCmId idPriv, boolean serverSide) throws IOException {
		super(endpointGroup, idPriv, serverSide);
		this.group = endpointGroup;
		this.getClusterId = group.newClusterId();
		this.freePostSend = new ArrayBlockingQueue<SendOperation>(group.getRpcpipeline());
	}

	public void init() throws IOException {
		super.init();
		for(int i = 0; i < group.getRpcpipeline(); i++){
			RpcServerEvent<R,T> event = new RpcServerEvent<R,T>(this, group.createRequest(), group.createResponse());
			this.eventPool.add(event);
			
		}
		RpcServerEvent<R,T> event = new RpcServerEvent<R,T>(this, group.createRequest(), group.createResponse());
		this.eventPool.add(event);		
	}
	
	void sendResponse(RpcServerEvent<R,T> event) throws IOException {
		if (sendMessage(event.getSendMessage(), event.getTicket())){
			eventPool.add(event);
		}
	}	
	
	public synchronized void dispatchCmEvent(RdmaCmEvent cmEvent) throws IOException {
		super.dispatchCmEvent(cmEvent);
		try {
			int eventType = cmEvent.getEvent();
			if (eventType == RdmaCmEvent.EventType.RDMA_CM_EVENT_DISCONNECTED.ordinal()) {
				logger.info("got disconnect event, dst " + this.getDstAddr() + ", src " + this.getSrcAddr());
				group.close(this);
			} 
		} catch (Exception e) {
			e.printStackTrace();
		}
	}	

	public int clusterId() {
		return getClusterId;
	}
	
	public void dispatchReceive(ByteBuffer recvBuffer, int ticket, SVCPostRecv postRecv) throws IOException {
		RpcServerEvent<R,T> event = eventPool.poll();
		if (event == null){
			logger.info("no free events, must be overrunning server.. ");
			throw new IOException("no free events, must be overrunning server.. ");
		}
		event.getReceiveMessage().update(recvBuffer);
		event.stamp(ticket);
		postRecv.execute();
		group.processServerEvent(event);			
	}
	
	public void dispatchSend(int ticket) throws IOException {
		freeSend(ticket);		
	}

	@Override
	public com.ibm.darpc.RpcEndpoint.SendOperation getSendSlot(ArrayBlockingQueue<com.ibm.darpc.RpcEndpoint.SendOperation> freePostSend)
			throws IOException {
		return null;
	}	

}
