package com.ibm.darpc;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.ArrayBlockingQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ibm.disni.rdma.verbs.RdmaCmEvent;
import com.ibm.disni.rdma.verbs.RdmaCmId;

public class DaRPCServerEndpoint<R extends DaRPCMessage, T extends DaRPCMessage> extends DaRPCEndpoint<R,T> {
	private static final Logger logger = LoggerFactory.getLogger("com.ibm.darpc");
	
	private DaRPCServerGroup<R, T> group;
	private int eventPoolSize;
	private ArrayBlockingQueue<DaRPCServerEvent<R,T>> eventPool;
	private ArrayBlockingQueue<DaRPCServerEvent<R,T>> lazyEvents;
	private int getClusterId;
	
	public DaRPCServerEndpoint(DaRPCServerGroup<R, T> group, RdmaCmId idPriv, boolean serverSide) throws IOException {
		super(group, idPriv, serverSide);
		this.group = group;
		this.getClusterId = group.newClusterId();
		this.eventPoolSize = Math.max(group.recvQueueSize(), group.sendQueueSize());
		this.eventPool = new ArrayBlockingQueue<DaRPCServerEvent<R,T>>(group.recvQueueSize());
		this.lazyEvents = new ArrayBlockingQueue<DaRPCServerEvent<R,T>>(group.recvQueueSize());

	}

	public void init() throws IOException {
		super.init();
		for(int i = 0; i < this.eventPoolSize; i++){
			DaRPCServerEvent<R,T> event = new DaRPCServerEvent<R,T>(this, group.createRequest(), group.createResponse());
			this.eventPool.add(event);
			
		}
	}
	
	void sendResponse(DaRPCServerEvent<R,T> event) throws IOException {
		if (sendMessage(event.getSendMessage(), event.getTicket())){
			eventPool.add(event);
		} else {
			lazyEvents.add(event);
		}
	}	
	
	public synchronized void dispatchCmEvent(RdmaCmEvent cmEvent) throws IOException {
		super.dispatchCmEvent(cmEvent);
		try {
			int eventType = cmEvent.getEvent();
			if (eventType == RdmaCmEvent.EventType.RDMA_CM_EVENT_ESTABLISHED.ordinal()) {
				logger.info("new RPC connection, eid " + this.getEndpointId());
				group.open(this);
			} else if (eventType == RdmaCmEvent.EventType.RDMA_CM_EVENT_DISCONNECTED.ordinal()) {
				logger.info("RPC disconnection, eid " + this.getEndpointId());
				group.close(this);
			} 
		} catch (Exception e) {
			e.printStackTrace();
		}
	}	

	public int clusterId() {
		return getClusterId;
	}
	
	public void dispatchReceive(ByteBuffer recvBuffer, int ticket, int recvIndex) throws IOException {
		DaRPCServerEvent<R,T> event = eventPool.poll();
		if (event == null){
			logger.info("no free events, must be overrunning server.. ");
			throw new IOException("no free events, must be overrunning server.. ");
		}
		event.getReceiveMessage().update(recvBuffer);
		event.stamp(ticket);
		postRecv(recvIndex);
		group.processServerEvent(event);			
	}
	
	public void dispatchSend(int ticket) throws IOException {
		freeSend(ticket);		
		DaRPCServerEvent<R,T> event = lazyEvents.poll();
		if (event != null){
			sendResponse(event);
		}
	}
}
