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
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ibm.disni.verbs.*;
import com.ibm.disni.endpoints.*;

public abstract class RpcClientEndpoint<R extends RdmaRpcMessage, T extends RdmaRpcMessage> extends RdmaClientEndpoint {
	private static final Logger logger = LoggerFactory.getLogger("com.ibm.darpc");
	
	public abstract RpcStream<R,T> createStream() throws IOException;
	public abstract SVCPostSend getSendSlot(ArrayBlockingQueue<SVCPostSend> freePostSend) throws IOException;
	
	//superset
	private RpcEndpointGroup<R, T> rpcGroup;
	
	private ByteBuffer[] recvBufs;
	private ByteBuffer[] sendBufs;
	private IbvMr[] recvMRs;
	private IbvMr[] sendMRs;
	private SVCPostRecv[] recvCall;
	private SVCPostSend[] sendCall;
	
	private ConcurrentHashMap<Integer, SVCPostSend> pendingPostSend;
	private ConcurrentHashMap<Integer, RpcFuture<R,T>> pendingFutures;
	private ArrayBlockingQueue<RpcServerEvent<R,T>> eventPool;
	private ArrayBlockingQueue<SVCPostSend> freePostSend;
	
	private AtomicLong ticketCount;
	private Object overFlowLock;
	private long maxInt;
	private int pipelineLength;
	private int bufferSize;
	private int maxinline;
	private int getClusterId;
	
	private AtomicLong messagesSent;
	private AtomicLong messagesReceived;
	
	
	public RpcClientEndpoint(RpcEndpointGroup<R, T> endpointGroup, RdmaCmId idPriv) throws IOException {
		super(endpointGroup, idPriv);
		this.rpcGroup = endpointGroup;
		this.maxinline = rpcGroup.getMaxInline();
		this.getClusterId = rpcGroup.newClusterId();
		this.bufferSize = rpcGroup.getBufferSize();
		this.pipelineLength = rpcGroup.getRpcpipeline();
		
		this.freePostSend = new ArrayBlockingQueue<SVCPostSend>(pipelineLength);
		this.pendingPostSend = new ConcurrentHashMap<Integer, SVCPostSend>();
		this.pendingFutures = new ConcurrentHashMap<Integer, RpcFuture<R,T>>();
		this.recvBufs = new ByteBuffer[pipelineLength];
		this.sendBufs = new ByteBuffer[pipelineLength];
		this.recvCall = new SVCPostRecv[pipelineLength];
		this.sendCall = new SVCPostSend[pipelineLength];
		this.recvMRs = new IbvMr[pipelineLength];
		this.sendMRs = new IbvMr[pipelineLength];
		
		this.eventPool = new ArrayBlockingQueue<RpcServerEvent<R,T>>(pipelineLength+1);
		this.overFlowLock = new Object();
		this.maxInt = (long) Integer.MAX_VALUE;
		
		this.ticketCount = new AtomicLong(0);
		this.messagesSent = new AtomicLong(0);
		this.messagesReceived = new AtomicLong(0);
		logger.info("RPC client endpoint, with buffer size = " + bufferSize + ", pipeline " + pipelineLength);
	}
	
	public void init() throws IOException {
		for(int i = 0; i < pipelineLength; i++){
			recvBufs[i] = ByteBuffer.allocateDirect(4 + bufferSize);
			sendBufs[i] = ByteBuffer.allocateDirect(4 + bufferSize);
			this.recvCall[i] = setupRecvTask(recvBufs[i], i);
			this.sendCall[i] = setupSendTask(sendBufs[i], i);
			RpcServerEvent<R,T> event = new RpcServerEvent<R,T>(this, rpcGroup.createRequest(), rpcGroup.createResponse());
			this.eventPool.add(event);
		}
		RpcServerEvent<R,T> event = new RpcServerEvent<R,T>(this, rpcGroup.createRequest(), rpcGroup.createResponse());
		this.eventPool.add(event);		
		
		for(int i = 0; i < pipelineLength; i++){
			freePostSend.add(sendCall[i]);
			recvCall[i].execute();
		}
	}
	
	@Override
	public synchronized void close() throws IOException, InterruptedException {
		for(int i = 0; i < pipelineLength; i++){
			deregisterMemory(recvMRs[i]);
			deregisterMemory(sendMRs[i]);
		}
		super.close();
	}	
	
	public int clusterId() {
		return getClusterId;
	}
	
	public long getMessagesSent() {
		return messagesSent.get();
	}
	
	public long getMessagesReceived() {
		return messagesReceived.get();
	}
	
	public synchronized void dispatchCmEvent(RdmaCmEvent cmEvent) throws IOException {
		super.dispatchCmEvent(cmEvent);
		try {
			int eventType = cmEvent.getEvent();
			if (eventType == RdmaCmEvent.EventType.RDMA_CM_EVENT_DISCONNECTED.ordinal()) {
				logger.info("got disconnect event, dst " + this.getDstAddr() + ", src " + this.getSrcAddr());
				rpcGroup.close(this);
			} 
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	public void dispatchCqEvent(IbvWC wc) throws IOException {
		if (wc.getOpcode() == 128){
			if (isServerSide()) {
				dispatchServerEvent(wc);
			} else {
				dispatchClientEvent(wc);
			}
			messagesReceived.incrementAndGet();
		} else if (wc.getOpcode() == 0) {
			dispatchSendEvent(wc);
		} else {
			throw new IOException("Unkown opcode " + wc.getOpcode());
		}		
	}
	
	void sendResponse(RpcServerEvent<R,T> event) throws IOException {
		sendMessage(event.getResponse(), event.getTicket());
		eventPool.add(event);
		this.freeSend(event.getTicket());
	}
	
	int sendRequest(RpcFuture<R,T> future) throws IOException {
		int ticket = getAndIncrement();
		future.stamp(ticket);
		pendingFutures.put(future.getTicket(), future);
		sendMessage(future.getRequest(), future.getTicket());
		return ticket;
	}
	
	private boolean sendMessage(RdmaRpcMessage message, int ticket) throws IOException {
		SVCPostSend postSend = getSendSlot(freePostSend);
		int index = (int) postSend.getWrMod(0).getWr_id();
		sendBufs[index].putInt(0, ticket);
		sendBufs[index].position(4);
		int written = 4 + message.write(sendBufs[index]);
		postSend.getWrMod(0).getSgeMod(0).setLength(written);
		postSend.getWrMod(0).setSend_flags(IbvSendWR.IBV_SEND_SIGNALED);
		if (written <= maxinline) {
			// inlining
			postSend.getWrMod(0).setSend_flags(postSend.getWrMod(0).getSend_flags() | IbvSendWR.IBV_SEND_INLINE);
		} 

		pendingPostSend.put(ticket, postSend);
		postSend.execute();
		messagesSent.incrementAndGet();

		return true;
	}
	
	private void dispatchServerEvent(IbvWC wc) throws IOException {
		if (wc.getStatus() == 5){
//			logger.info("flush wc");
		} else if (wc.getStatus() != 0){
			logger.info("faulty request, status " + wc.getStatus());
		} else {
			//assuming wc.getOpcode() == 128, endpoint.isServerSide() = true
			int index = (int) wc.getWr_id();
			ByteBuffer recvBuffer = recvBufs[index];
			SVCPostRecv postRecv = recvCall[index];
			RpcServerEvent<R,T> event = eventPool.poll();
			if (event == null){
				logger.info("no free events, must be overrunning server.. , messages sent " + messagesSent.get() + ", messagesReceived " + messagesReceived.get());
				throw new IOException("no free events, must be overrunning server.. , messages sent " + messagesSent.get() + ", messagesReceived " + messagesReceived.get());
			}
			int ticket = recvBuffer.getInt(0);
			recvBuffer.position(4);
			event.getRequest().update(recvBuffer);
			postRecv.execute();
			event.stamp(ticket);
			rpcGroup.processServerEvent(event);			
		}
	}	

	private void dispatchClientEvent(IbvWC wc) throws IOException {
		if (wc.getStatus() == 5){
//			logger.info("flush wc");
		} else if (wc.getStatus() != 0){
			logger.info("faulty request, status " + wc.getStatus());
		} else {
			// assuming wc.getOpcode() == 128, endpoint.isServerSide() = false
			int index = (int) wc.getWr_id();
			ByteBuffer recvBuffer = recvBufs[index];
			int ticket = recvBuffer.getInt(0);
			recvBuffer.position(4);
			RpcFuture<R, T> future = pendingFutures.remove(ticket);
			future.getResponse().update(recvBuffer);
			SVCPostRecv postRecv = recvCall[index];
			postRecv.execute();
			future.signal(wc.getStatus());
			freeSend(ticket);
		}
	}
	
	private void dispatchSendEvent(IbvWC wc) throws IOException {
		if (isServerSide()){
			if (wc.getStatus() == 5){
//				logger.info("flush wc");
			} else if (wc.getStatus() != 0){
				logger.info("faulty request, status " + wc.getStatus());
			} else {
				//assuming wc.getOpcode() == 0
//				int index = (int) wc.getWr_id();
//				ByteBuffer sendBuffer = sendBufs[index];
//				int ticket = sendBuffer.getInt(0);				
//				freeSend(ticket);
			}
		} else {
//			logger.info("sending client bytes " + wc.getByte_len() + ", wrid " + wc.getWr_id() + ", opcode " + wc.getOpcode()  + ", ep-id " + this.getEndpointId());
		}
	}
	
	private void freeSend(int ticket) throws IOException {
		SVCPostSend _postSend = pendingPostSend.remove(ticket);
		if (_postSend == null) {
			throw new IOException("no pending ticket " + ticket + ", current ticket count " + ticketCount.get());
		}
		this.freePostSend.add(_postSend);
	}

	private SVCPostSend setupSendTask(ByteBuffer sendBuf, int wrid) throws IOException {
		ArrayList<IbvSendWR> sendWRs = new ArrayList<IbvSendWR>(1);
		LinkedList<IbvSge> sgeList = new LinkedList<IbvSge>();
		
		IbvMr mr = registerMemory(sendBuf).execute().free().getMr();
		sendMRs[wrid] = mr;
		IbvSge sge = new IbvSge();
		sge.setAddr(mr.getAddr());
		sge.setLength(mr.getLength());
		int lkey = mr.getLkey();
		sge.setLkey(lkey);
		sgeList.add(sge);
	
		IbvSendWR sendWR = new IbvSendWR();
		sendWR.setSg_list(sgeList);
		sendWR.setWr_id(wrid);
		sendWRs.add(sendWR);
		sendWR.setSend_flags(IbvSendWR.IBV_SEND_SIGNALED);
		sendWR.setOpcode(IbvSendWR.IbvWrOcode.IBV_WR_SEND.ordinal());
		
		return postSend(sendWRs);
	}

	private SVCPostRecv setupRecvTask(ByteBuffer recvBuf, int wrid) throws IOException {
		ArrayList<IbvRecvWR> recvWRs = new ArrayList<IbvRecvWR>(1);
		LinkedList<IbvSge> sgeList = new LinkedList<IbvSge>();
		
		IbvMr mr = registerMemory(recvBuf).execute().free().getMr();
		recvMRs[wrid] = mr;
		IbvSge sge = new IbvSge();
		sge.setAddr(mr.getAddr());
		sge.setLength(mr.getLength());
		int lkey = mr.getLkey();
		sge.setLkey(lkey);
		sgeList.add(sge);
		
		IbvRecvWR recvWR = new IbvRecvWR();
		recvWR.setWr_id(wrid);
		recvWR.setSg_list(sgeList);
		recvWRs.add(recvWR);
	
		return postRecv(recvWRs);
	}

	private int getAndIncrement() {
		long _value = ticketCount.getAndIncrement();
		
		if (_value >= maxInt){
			synchronized(overFlowLock){
				_value = ticketCount.get();
				if (_value >= maxInt){
					ticketCount.set(0);
				}
			}
			_value = ticketCount.getAndIncrement();
		}
		
		long value = _value % maxInt;
		return (int) value;
	}
}
