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
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ibm.disni.verbs.*;

public class RpcPassiveClientEndpoint<R extends RdmaRpcMessage, T extends RdmaRpcMessage> extends RpcClientEndpoint<R,T> {
	private static Logger logger = LoggerFactory.getLogger("com.ibm.darpc");
	
	private int streamCount;
	private IbvWC[] wcList;
	private SVCPollCq poll;	
	private ReentrantLock lock;
	
	public RpcPassiveClientEndpoint(RpcEndpointGroup<R, T> endpointGroup,
			RdmaCmId idPriv) throws IOException {
		super(endpointGroup, idPriv);
		this.streamCount = 1;
		this.lock = new ReentrantLock();
	}

	@Override
	public RpcStream<R, T> createStream() throws IOException {
		int streamId = this.streamCount;
		RpcStream<R,T> stream = new RpcStreamPassive<R,T>(this, streamId);
		streamCount++;
		return stream;
	}
	
	@Override
	public void init() throws IOException {
		// TODO Auto-generated method stub
		super.init();
		IbvCQ cq = getCqProvider().getCQ();
		this.wcList = new IbvWC[getCqProvider().getCqSize()];
		for (int i = 0; i < wcList.length; i++){
			wcList[i] = new IbvWC();
		}		
		this.poll = cq.poll(wcList, wcList.length);			
	}	
	
	@Override
	public SendOperation getSendSlot(ArrayBlockingQueue<SendOperation> freePostSend) throws IOException {
		SendOperation sendOperation = freePostSend.poll();
		while(sendOperation == null){
			pollOnce();
			sendOperation = freePostSend.poll();
		}
		return sendOperation;
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
