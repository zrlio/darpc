package com.ibm.darpc;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.NoSuchElementException;

import com.ibm.disni.rdma.RdmaEndpoint;

public interface DaRPCMemPool<E extends DaRPCEndpoint<R,T>, R extends DaRPCMessage, T extends DaRPCMessage> {
	void init(DaRPCEndpointGroup<E,R,T> endpointGroup);
	void close() throws IOException;
	ByteBuffer getBuffer(RdmaEndpoint endpoint) throws IOException, NoSuchElementException;
	void freeBuffer(RdmaEndpoint endpoint, ByteBuffer buffer) throws IOException;
	public int getLKey(ByteBuffer b) throws IllegalArgumentException;
}
