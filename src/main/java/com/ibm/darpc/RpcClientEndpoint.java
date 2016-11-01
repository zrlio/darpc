package com.ibm.darpc;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

import com.ibm.disni.rdma.verbs.IbvCQ;
import com.ibm.disni.rdma.verbs.IbvWC;
import com.ibm.disni.rdma.verbs.RdmaCmId;
import com.ibm.disni.rdma.verbs.SVCPollCq;
import com.ibm.disni.rdma.verbs.SVCPostRecv;

public class RpcClientEndpoint<R extends RpcMessage, T extends RpcMessage> extends RpcEndpoint<R,T> {
	private ConcurrentHashMap<Integer, RpcFuture<R,T>> pendingFutures;
	private AtomicInteger ticketCount;
	private int streamCount;
	private IbvWC[] wcList;
	private SVCPollCq poll;	
	private ReentrantLock lock;	

	public RpcClientEndpoint(RpcEndpointGroup<? extends RpcClientEndpoint<R, T>, R, T> endpointGroup, RdmaCmId idPriv, boolean serverSide) throws IOException {
		super(endpointGroup, idPriv, serverSide);
		this.pendingFutures = new ConcurrentHashMap<Integer, RpcFuture<R,T>>();
		this.ticketCount = new AtomicInteger(0);
		this.streamCount = 1;
		this.lock = new ReentrantLock();		
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
	
	public RpcStream<R, T> createStream() throws IOException {
		int streamId = this.streamCount;
		RpcStream<R,T> stream = new RpcStream<R,T>(this, streamId);
		streamCount++;
		return stream;
	}	

	@Override
	public com.ibm.darpc.RpcEndpoint.SendOperation getSendSlot(
			ArrayBlockingQueue<com.ibm.darpc.RpcEndpoint.SendOperation> freePostSend)
			throws IOException {
		return null;
	}

	@Override
	public void dispatchReceive(ByteBuffer recvBuffer, int ticket, SVCPostRecv postRecv) throws IOException {
		RpcFuture<R, T> future = pendingFutures.remove(ticket);
		future.getReceiveMessage().update(recvBuffer);
		postRecv.execute();
		future.signal(0);
		freeSend(ticket);
	}

	@Override
	public void dispatchSend(int ticket) throws IOException {
		freeSend(ticket);
	}
	
	int sendRequest(RpcFuture<R,T> future) throws IOException {
		int ticket = getAndIncrement();
		future.stamp(ticket);
		pendingFutures.put(future.getTicket(), future);
		sendMessage(future.getSendMessage(), future.getTicket());
		return ticket;
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
