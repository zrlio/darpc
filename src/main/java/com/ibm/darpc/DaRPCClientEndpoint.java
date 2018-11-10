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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.ibm.disni.verbs.IbvCQ;
import com.ibm.disni.verbs.IbvWC;
import com.ibm.disni.verbs.RdmaCmId;
import com.ibm.disni.verbs.SVCPollCq;

public class DaRPCClientEndpoint<R extends DaRPCMessage, T extends DaRPCMessage> extends DaRPCEndpoint<R,T> {
	private static final Logger logger = LoggerFactory.getLogger("com.ibm.darpc");
	
	private ConcurrentHashMap<Integer, DaRPCFuture<R,T>> pendingFutures;
	private AtomicInteger ticketCount;
	private int streamCount;
	private IbvWC[] wcList;
	private SVCPollCq poll;	
	private ReentrantLock lock;	

	public DaRPCClientEndpoint(DaRPCEndpointGroup<? extends DaRPCClientEndpoint<R, T>, R, T> group, RdmaCmId idPriv, boolean serverSide) throws IOException {
		super(group, idPriv, serverSide);
		this.pendingFutures = new ConcurrentHashMap<Integer, DaRPCFuture<R,T>>();
		this.ticketCount = new AtomicInteger(0);
		this.streamCount = 1;
		this.lock = new ReentrantLock();		
	}
	
	@Override
	public void init() throws IOException {
		super.init();
		IbvCQ cq = getCqProvider().getCQ();
		this.wcList = new IbvWC[getCqProvider().getCqSize()];
		for (int i = 0; i < wcList.length; i++){
			wcList[i] = new IbvWC();
		}		
		this.poll = cq.poll(wcList, wcList.length);			
	}	
	
	public DaRPCStream<R, T> createStream() throws IOException {
		int streamId = this.streamCount;
		DaRPCStream<R,T> stream = new DaRPCStream<R,T>(this, streamId);
		streamCount++;
		return stream;
	}	
	
	int sendRequest(DaRPCFuture<R,T> future) throws IOException {
		int ticket = getAndIncrement();
		future.stamp(ticket);
		pendingFutures.put(future.getTicket(), future);
		while (!sendMessage(future.getSendMessage(), future.getTicket())){
			pollOnce();
		}
		return ticket;
	}		

	@Override
	public void dispatchReceive(ByteBuffer recvBuffer, int ticket, int recvIndex) throws IOException {
		DaRPCFuture<R, T> future = pendingFutures.get(ticket);
		if (future == null){
			logger.info("no pending future (receive) for ticket " + ticket);
			throw new IOException("no pending future (receive) for ticket " + ticket);
		}		
		future.getReceiveMessage().update(recvBuffer);
		postRecv(recvIndex);
		if (future.touch()){
			pendingFutures.remove(ticket);
			freeSend(ticket);
		}
		future.signal(0);
	}

	@Override
	public void dispatchSend(int ticket) throws IOException {
		DaRPCFuture<R, T> future = pendingFutures.get(ticket);
		if (future == null){
			logger.info("no pending future (send) for ticket " + ticket);
			throw new IOException("no pending future (send) for ticket " + ticket);
		}		
		if (future.touch()){
			pendingFutures.remove(ticket);
			freeSend(ticket);
		}
	}
	
	private int getAndIncrement() {
		return ticketCount.getAndIncrement() & Integer.MAX_VALUE;
	}	

	public void pollOnce() throws IOException {
		if (!lock.tryLock()){
			return;
		}
		
		try {
			_pollOnce();
		} finally {
			lock.unlock();
		}
	}
	
	public void pollUntil(AtomicInteger future, long timeout) throws IOException {
		boolean locked = false;
		while(true){
			locked = lock.tryLock();
			if (future.get() > 0 || locked){
				break;
			}
		}

		try {
			if (future.get() == 0){
				_pollUntil(future, timeout);
			}
		} finally {
			if (locked){
				lock.unlock();
			}
		}
	}	
	
	private int _pollOnce() throws IOException {
		int res = poll.execute().getPolls();
		if (res > 0) {
			for (int i = 0; i < res; i++){
				IbvWC wc = wcList[i];
				dispatchCqEvent(wc);
			}
			
		} 
		return res;
	}
	
	private int _pollUntil(AtomicInteger future, long timeout) throws IOException {
		long count = 0;
		final long checkTimeOut = 1 << 14 /* 16384 */;
		long startTime = System.nanoTime();
		while(future.get() == 0){
			int res = poll.execute().getPolls();
			if (res > 0){
				for (int i = 0; i < res; i++){
					IbvWC wc = wcList[i];
					dispatchCqEvent(wc);
				}
			}
			if (count == checkTimeOut) {
				count = 0;
				if ((System.nanoTime() - startTime) / 1e6 > timeout) {
					break;
				}
			}			
			count++;			
		}
		return 1;
	}	

}
