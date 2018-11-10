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
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ibm.disni.verbs.*;
import com.ibm.disni.util.MemoryUtils;
import com.ibm.disni.*;

public abstract class DaRPCEndpoint<R extends DaRPCMessage, T extends DaRPCMessage> extends RdmaEndpoint {
	private static final Logger logger = LoggerFactory.getLogger("com.ibm.darpc");
	private static final int headerSize = 4;
	
	public abstract void dispatchReceive(ByteBuffer buffer, int ticket, int recvIndex) throws IOException;
	public abstract void dispatchSend(int ticket) throws IOException;
	
	private DaRPCEndpointGroup<? extends DaRPCEndpoint<R,T>, R, T> rpcGroup;
	private ByteBuffer dataBuffer;
	private IbvMr dataMr;
	private ByteBuffer receiveBuffer;
	private ByteBuffer sendBuffer;
	private ByteBuffer[] recvBufs;
	private ByteBuffer[] sendBufs;
	private SVCPostRecv[] recvCall;
	private SVCPostSend[] sendCall;
	private ConcurrentHashMap<Integer, SVCPostSend> pendingPostSend;
	private ArrayBlockingQueue<SVCPostSend> freePostSend;
	private AtomicLong ticketCount;
	private int pipelineLength;
	private int payloadSize;
	private int rawBufferSize;
	private int maxinline;
	private AtomicLong messagesSent;
	private AtomicLong messagesReceived;
	
	
	public DaRPCEndpoint(DaRPCEndpointGroup<? extends DaRPCEndpoint<R,T>, R, T> endpointGroup, RdmaCmId idPriv, boolean serverSide) throws IOException {
		super(endpointGroup, idPriv, serverSide);
		this.rpcGroup = endpointGroup;
		this.maxinline = rpcGroup.getMaxInline();
		this.payloadSize = rpcGroup.getBufferSize();
		this.rawBufferSize = headerSize + this.payloadSize;
		this.pipelineLength = rpcGroup.recvQueueSize();
		this.freePostSend = new ArrayBlockingQueue<SVCPostSend>(pipelineLength);
		this.pendingPostSend = new ConcurrentHashMap<Integer, SVCPostSend>();
		this.recvBufs = new ByteBuffer[pipelineLength];
		this.sendBufs = new ByteBuffer[pipelineLength];
		this.recvCall = new SVCPostRecv[pipelineLength];
		this.sendCall = new SVCPostSend[pipelineLength];
		this.ticketCount = new AtomicLong(0);
		this.messagesSent = new AtomicLong(0);
		this.messagesReceived = new AtomicLong(0);
		logger.info("RPC client endpoint, with payload buffer size = " + payloadSize + ", pipeline " + pipelineLength);
	}
	
	public void init() throws IOException {
		int sendBufferOffset = pipelineLength * rawBufferSize;

		/* Main data buffer for sends and receives. Will be split into two regions,
		 * one for sends and one for receives.
		 */
		dataBuffer = ByteBuffer.allocateDirect(pipelineLength * rawBufferSize * 2);
		/* Only do one memory registration with the IB card. */
		dataMr = registerMemory(dataBuffer).execute().free().getMr();

		/* Receive memory region is the first half of the main buffer. */
		dataBuffer.limit(dataBuffer.position() + sendBufferOffset);
		receiveBuffer = dataBuffer.slice();

		/* Send memory region is the second half of the main buffer. */
		dataBuffer.position(sendBufferOffset);
		dataBuffer.limit(dataBuffer.position() + sendBufferOffset);
		sendBuffer = dataBuffer.slice();

		for(int i = 0; i < pipelineLength; i++) {
			/* Create single receive buffers within the receive region in form of slices. */
			receiveBuffer.position(i * rawBufferSize);
			receiveBuffer.limit(receiveBuffer.position() + rawBufferSize);
			recvBufs[i] = receiveBuffer.slice();

			/* Create single send buffers within the send region in form of slices. */
			sendBuffer.position(i * rawBufferSize);
			sendBuffer.limit(sendBuffer.position() + rawBufferSize);
			sendBufs[i] = sendBuffer.slice();

			this.recvCall[i] = setupRecvTask(i);
			this.sendCall[i] = setupSendTask(i);
			freePostSend.add(sendCall[i]);
			recvCall[i].execute();
		}
	}

	@Override
	public synchronized void close() throws IOException, InterruptedException {
		super.close();
		deregisterMemory(dataMr);
	}	
	
	public long getMessagesSent() {
		return messagesSent.get();
	}
	
	public long getMessagesReceived() {
		return messagesReceived.get();
	}
	
	protected boolean sendMessage(DaRPCMessage message, int ticket) throws IOException {
		SVCPostSend postSend = freePostSend.poll();
		if (postSend != null){
			int index = (int) postSend.getWrMod(0).getWr_id();
			sendBufs[index].putInt(0, ticket);
			sendBufs[index].position(4);
			int written = 4 + message.write(sendBufs[index]);
			postSend.getWrMod(0).getSgeMod(0).setLength(written);
			postSend.getWrMod(0).setSend_flags(IbvSendWR.IBV_SEND_SIGNALED);
			if (written <= maxinline) {
				postSend.getWrMod(0).setSend_flags(postSend.getWrMod(0).getSend_flags() | IbvSendWR.IBV_SEND_INLINE);
			} 
			pendingPostSend.put(ticket, postSend);
			postSend.execute();
			messagesSent.incrementAndGet();
			return true;
		} else {
			return false;
		}
	}
	
	protected void postRecv(int index) throws IOException {
		recvCall[index].execute();
	}	
	
	public void freeSend(int ticket) throws IOException {
		SVCPostSend sendOperation = pendingPostSend.remove(ticket);
		if (sendOperation == null) {
			throw new IOException("no pending ticket " + ticket + ", current ticket count " + ticketCount.get());
		}
		this.freePostSend.add(sendOperation);
	}	
	
	public void dispatchCqEvent(IbvWC wc) throws IOException {
		if (wc.getStatus() == 5){
			//flush
			return;
		} else if (wc.getStatus() != 0){
			throw new IOException("Faulty operation! wc.status " + wc.getStatus());
		} 	
		
		if (wc.getOpcode() == 128){
			//receiving a message
			int index = (int) wc.getWr_id();
			ByteBuffer recvBuffer = recvBufs[index];
			int ticket = recvBuffer.getInt(0);
			recvBuffer.position(4);
			dispatchReceive(recvBuffer, ticket, index);
		} else if (wc.getOpcode() == 0) {
			//send completion
			int index = (int) wc.getWr_id();
			ByteBuffer sendBuffer = sendBufs[index];
			int ticket = sendBuffer.getInt(0);
			dispatchSend(ticket);
		} else {
			throw new IOException("Unkown opcode " + wc.getOpcode());
		}		
	}	
	
	private SVCPostSend setupSendTask(int wrid) throws IOException {
		ArrayList<IbvSendWR> sendWRs = new ArrayList<IbvSendWR>(1);
		LinkedList<IbvSge> sgeList = new LinkedList<IbvSge>();

		IbvSge sge = new IbvSge();
		sge.setAddr(MemoryUtils.getAddress(sendBufs[wrid]));
		sge.setLength(rawBufferSize);
		sge.setLkey(dataMr.getLkey());
		sgeList.add(sge);

		IbvSendWR sendWR = new IbvSendWR();
		sendWR.setSg_list(sgeList);
		sendWR.setWr_id(wrid);
		sendWRs.add(sendWR);
		sendWR.setSend_flags(IbvSendWR.IBV_SEND_SIGNALED);
		sendWR.setOpcode(IbvSendWR.IbvWrOcode.IBV_WR_SEND.ordinal());

		return postSend(sendWRs);
	}

	private SVCPostRecv setupRecvTask(int wrid) throws IOException {
		ArrayList<IbvRecvWR> recvWRs = new ArrayList<IbvRecvWR>(1);
		LinkedList<IbvSge> sgeList = new LinkedList<IbvSge>();

		IbvSge sge = new IbvSge();
		sge.setAddr(MemoryUtils.getAddress(recvBufs[wrid]));
		sge.setLength(rawBufferSize);
		sge.setLkey(dataMr.getLkey());
		sgeList.add(sge);

		IbvRecvWR recvWR = new IbvRecvWR();
		recvWR.setWr_id(wrid);
		recvWR.setSg_list(sgeList);
		recvWRs.add(recvWR);

		return postRecv(recvWRs);
	}
}
