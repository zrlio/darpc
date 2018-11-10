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
import java.nio.ByteBuffer;
import java.util.concurrent.ArrayBlockingQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ibm.disni.verbs.RdmaCmEvent;
import com.ibm.disni.verbs.RdmaCmId;

public class DaRPCServerEndpoint<R extends DaRPCMessage, T extends DaRPCMessage> extends DaRPCEndpoint<R,T> {
	private static final Logger logger = LoggerFactory.getLogger("com.ibm.darpc");
	
	private DaRPCServerGroup<R, T> group;
	private ArrayBlockingQueue<DaRPCServerEvent<R,T>> eventPool;
	private ArrayBlockingQueue<DaRPCServerEvent<R,T>> lazyEvents;
	private int getClusterId;
	
	public DaRPCServerEndpoint(DaRPCServerGroup<R, T> group, RdmaCmId idPriv, boolean serverSide) throws IOException {
		super(group, idPriv, serverSide);
		this.group = group;
		this.getClusterId = group.newClusterId();
		this.eventPool = new ArrayBlockingQueue<DaRPCServerEvent<R,T>>(group.recvQueueSize());
		this.lazyEvents = new ArrayBlockingQueue<DaRPCServerEvent<R,T>>(group.recvQueueSize());
	}

	public void init() throws IOException {
		super.init();
		for(int i = 0; i < group.recvQueueSize(); i++){
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
