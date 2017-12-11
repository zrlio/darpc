package com.ibm.darpc;

import java.io.IOException;
import java.nio.ByteBuffer;

import com.ibm.disni.rdma.RdmaEndpoint;

public interface DaRPCMemPool {
	void close() throws IOException;
	ByteBuffer getBuffer(RdmaEndpoint endpoint, int size) throws IOException;
	void freeBuffer(RdmaEndpoint endpoint, ByteBuffer b) throws IOException;
	public int getLKey(RdmaEndpoint endpoint, ByteBuffer b) throws IllegalArgumentException;
}
