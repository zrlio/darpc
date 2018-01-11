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
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ibm.disni.rdma.RdmaEndpoint;
import com.ibm.disni.rdma.verbs.IbvMr;
import com.ibm.disni.rdma.verbs.IbvPd;
import com.ibm.disni.util.MemoryUtils;

public class DaRPCMemPoolImpl<E extends DaRPCEndpoint<R,T>, R extends DaRPCMessage, T extends DaRPCMessage> implements DaRPCMemPool<E,R,T> {
	private static final Logger logger = LoggerFactory.getLogger("com.ibm.darpc");
	private static final int defaultAllocationSize = 16 * 1024 * 1024; // 16MB
	private final int allocationSize;
	private final int alignmentSize;
	private final int allocationLimit;
	private int currentAllocationSize;
	private String hugePageFile;
	private ConcurrentHashMap<Long, IbvMr> memoryRegions;
	private int access;
	private DaRPCEndpointGroup<E,R,T> endpointGroup;
	private ConcurrentHashMap<IbvPd, LinkedBlockingQueue<ByteBuffer>> pdMap;
	private List<IbvMr> mrs;

	public DaRPCMemPoolImpl(String hugePagePath, int allocationSize, int alignmentSize, int allocationLimit) throws IllegalArgumentException {
		if (hugePagePath == null) {
			logger.error("Hugepage path must be set");
			throw new IllegalArgumentException("Hugepage path must be set");
		}
		this.allocationSize = allocationSize;
		this.alignmentSize = alignmentSize;
		this.allocationLimit = allocationLimit;
		hugePageFile = hugePagePath + "/darpcmempoolimpl.mem";
		this.currentAllocationSize = 0;
		this.access = IbvMr.IBV_ACCESS_LOCAL_WRITE | IbvMr.IBV_ACCESS_REMOTE_WRITE | IbvMr.IBV_ACCESS_REMOTE_READ;
		this.pdMap = new ConcurrentHashMap<IbvPd, LinkedBlockingQueue<ByteBuffer>>();
		this.mrs = new LinkedList<IbvMr>();
	}

	public DaRPCMemPoolImpl(String hugePagePath) throws IllegalArgumentException {
		this(hugePagePath, defaultAllocationSize, 0, 16 * defaultAllocationSize);
	}

	@Override
	public void init(DaRPCEndpointGroup<E,R,T> endpointGroup) {
		this.endpointGroup = endpointGroup;
	}

	@Override
	public void close() throws IOException {
		synchronized(this) {
			for (IbvMr m : mrs) {
				try {
					m.deregMr().execute().free();
				} catch (IOException e) {
					System.out.println("Could not unregister memory region.");
					e.printStackTrace();
				}
			}
			mrs = null;
			File f = new File(hugePageFile);
			f.delete();
		}
	}

	@Override
	public ByteBuffer getBuffer(RdmaEndpoint endpoint) throws IOException, NoSuchElementException {
		LinkedBlockingQueue<ByteBuffer> freeList = pdMap.get(endpoint.getPd());

		if (freeList == null) {
			synchronized(this) {
				freeList = pdMap.get(endpoint.getPd());
				if (freeList == null) {
					freeList = new LinkedBlockingQueue<ByteBuffer>();
					pdMap.put(endpoint.getPd(), freeList);
				}
			}
		}

		ByteBuffer r = freeList.poll();

		if (r == null) {
			synchronized(this) {
				r = freeList.poll();
				if (r == null) {
					allocateHugePageBuffer(freeList, endpoint.getPd());
				}
				r = freeList.poll();
				if (r == null) {
					logger.error("Failed to allocate more buffers.");
					throw new NoSuchElementException("Failed to allocate more buffers.");
				}
			}
		}
		r.clear();
		return r;
	}

	@Override
	public void freeBuffer(RdmaEndpoint endpoint, ByteBuffer buffer) {
		LinkedBlockingQueue<ByteBuffer> freeList = pdMap.get(endpoint.getPd());
		freeList.add(buffer);
	}

	@Override
	public int getLKey(ByteBuffer buffer) throws IllegalArgumentException {
		return memoryRegions.get(MemoryUtils.getAddress(buffer)).getLkey();
	}

	// allocate a buffer from hugepages
	private void allocateHugePageBuffer(LinkedBlockingQueue<ByteBuffer> freeList, IbvPd pd) throws IOException {
		int totalAllocationSize = allocationSize + alignmentSize;
		if ((currentAllocationSize + totalAllocationSize) > allocationLimit) {
			logger.error("Out of memory. Cannot allocate more buffers from hugepages.");
			throw new IOException("Out of memory. Cannot allocate more buffers from hugepages.");
		}
		RandomAccessFile randomFile = null;
		try {
			randomFile = new RandomAccessFile(hugePageFile, "rw");
		} catch (FileNotFoundException e) {
			logger.error("Path " + hugePageFile + " to huge page path/file cannot be accessed.");
			throw e;
		}
		try {
			randomFile.setLength(totalAllocationSize);
		} catch (IOException e) {
			logger.error("Could not set allocation length of mapped random access file on huge page directory.");
			logger.error("allocaiton size = " + allocationSize + " , alignment size =  " + alignmentSize);
			logger.error("allocation size and alignment must be a multiple of the hugepage size.");
			randomFile.close();
			throw e;
		}
		FileChannel channel = randomFile.getChannel();
		MappedByteBuffer mappedBuffer = null;
		try {
			mappedBuffer = channel.map(MapMode.READ_WRITE, 0,
					totalAllocationSize);
		} catch (IOException e) {
			logger.error("Could not map the huge page file on path " + hugePageFile);
			randomFile.close();
			throw e;
		}
		randomFile.close();

		currentAllocationSize += totalAllocationSize;

		long rawBufferAddress = MemoryUtils.getAddress(mappedBuffer);
		if (alignmentSize > 0) {
			long alignmentOffset = rawBufferAddress % alignmentSize;
			if (alignmentOffset != 0) {
				mappedBuffer.position(alignmentSize - (int)alignmentOffset);
			}
		}

		ByteBuffer alignedBuffer = mappedBuffer.slice();

		IbvMr mr = pd.regMr(alignedBuffer, access).execute().free().getMr();
		mrs.add(mr);
		int sliceSize = endpointGroup.getBufferSize() + DaRPCEndpoint.HEADERSIZE;
		int i = 0;
		while ((i * sliceSize + sliceSize) < alignedBuffer.capacity()) {
			alignedBuffer.position(i * sliceSize);
			alignedBuffer.limit(i * sliceSize + sliceSize);
			ByteBuffer buffer = alignedBuffer.slice();
			freeList.add(buffer);
			memoryRegions.put(MemoryUtils.getAddress(buffer), mr);
			i++;
		}
	}
}
