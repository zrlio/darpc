package com.ibm.darpc;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.util.LinkedList;
import java.util.NoSuchElementException;

import com.ibm.disni.rdma.RdmaEndpoint;
import com.ibm.disni.rdma.verbs.IbvMr;
import com.ibm.disni.rdma.verbs.IbvPd;
import com.ibm.disni.util.MemoryUtils;

public class DaRPCMemPoolImpl implements DaRPCMemPool {
	private static final int defaultAllocationSize = 16 * 1024 * 1024; // 16MB
	private final int allocationSize;
	private final int alignmentSize;
	private String hugePageFile;
	int offset;
	ByteBuffer byteBuffer;
	IbvPd pd;
	IbvMr mr;
	int access;
	LinkedList<ByteBuffer> freeList;

	public DaRPCMemPoolImpl(String hugePagePath, int allocationSize, int alignmentSize) throws IllegalArgumentException {
		if (hugePagePath == null) {
			System.out.println("Hugepage path must be set");
			throw new IllegalArgumentException("Hugepage path must be set");
		}

		this.allocationSize = allocationSize;
		this.alignmentSize = alignmentSize;
		hugePageFile = hugePagePath + "/darpcmempoolimpl.mem";

		this.access = IbvMr.IBV_ACCESS_LOCAL_WRITE | IbvMr.IBV_ACCESS_REMOTE_WRITE | IbvMr.IBV_ACCESS_REMOTE_READ;
	}

	public DaRPCMemPoolImpl(String hugePagePath) throws IllegalArgumentException {
		this(hugePagePath, defaultAllocationSize, 0);
	}

	// allocate a buffer from hugepages
	ByteBuffer allocateHugePageBuffer() throws IOException {
		RandomAccessFile randomFile = null;
		try {
			randomFile = new RandomAccessFile(hugePageFile, "rw");
		} catch (FileNotFoundException e) {
			System.out.println("Path " + hugePageFile + " to huge page path/file cannot be accessed.");
			throw e;
		}
		try {
			randomFile.setLength(allocationSize + alignmentSize);
		} catch (IOException e) {
			System.out.println("Coult not set allocation length of mapped random access file on huge page directory.");
			System.out.println("allocaiton size = " + allocationSize + " , alignment size =  " + alignmentSize);
			System.out.println("allocation size and alignment must be a multiple of the hugepage size.");
			randomFile.close();
			throw e;
		}
		FileChannel channel = randomFile.getChannel();
		MappedByteBuffer mappedBuffer = null;
		try {
			mappedBuffer = channel.map(MapMode.READ_WRITE, 0,
					allocationSize + alignmentSize);
		} catch (IOException e) {
			System.out.println("Could not map the huge page file on path " + hugePageFile);
			randomFile.close();
			throw e;
		}
		randomFile.close();

		long rawBufferAddress = MemoryUtils.getAddress(mappedBuffer);
		if (alignmentSize > 0) {
			long alignmentOffset = rawBufferAddress % alignmentSize;
			if (alignmentOffset != 0) {
				mappedBuffer.position(alignmentSize - (int)alignmentOffset);
			}
		}
		ByteBuffer b = mappedBuffer.slice();
		return (b);
	}


	@Override
	public void close() throws IOException {
		synchronized(this) {
			try {
				mr.deregMr().execute().free();
			} catch (IOException e) {
				System.out.println("Could not unregister memory region.");
				e.printStackTrace();
			}
			File f = new File(hugePageFile);
			f.delete();
		}
	}

	@Override
	public ByteBuffer getBuffer(RdmaEndpoint endpoint, int size) throws IOException, NoSuchElementException {
		ByteBuffer r = null;

		synchronized(this) {
			if (pd == null) {
				pd = endpoint.getPd();
			} else if (!pd.equals(endpoint.getPd())) {
				throw new IOException("No support for more than one PD");
			}
			if (mr == null) {
				byteBuffer = allocateHugePageBuffer();
				mr = pd.regMr(byteBuffer, access).execute().free().getMr();
			}

			if (freeList == null) {
				offset = size;
				freeList = new LinkedList<ByteBuffer>();
				int i = 0;
				while ((i * offset + offset) < byteBuffer.capacity()) {
					byteBuffer.position(i * offset);
					byteBuffer.limit(i * offset + offset);
					ByteBuffer b = byteBuffer.slice();
					freeList.addLast(b);
					i++;
				}
			}
			else
			{
				if (size != offset) {
					throw new IOException("Requested size does not match block size managed by memory pool.");
				}
			}
			r = freeList.removeFirst();
			r.clear();
		}
		return r;
	}

	@Override
	public void freeBuffer(RdmaEndpoint endpoint, ByteBuffer b) {
		synchronized(this) {
			freeList.addLast(b);
		}
	}

	@Override
	public int getLKey(RdmaEndpoint endpoint, ByteBuffer b) throws IllegalArgumentException {
		return mr.getLkey();
	}
}
