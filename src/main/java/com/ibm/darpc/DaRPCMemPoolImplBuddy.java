package com.ibm.darpc;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import com.ibm.disni.rdma.RdmaEndpoint;
import com.ibm.disni.rdma.verbs.IbvMr;
import com.ibm.disni.rdma.verbs.IbvPd;
import com.ibm.disni.util.MemoryUtils;

public class DaRPCMemPoolImplBuddy implements DaRPCMemPool {
	final static int defaultAllocationSize = 16 * 1024 * 1024; // 16MB
	final static int defaultMinAllocationSize = 4 * 1024; // 4KB
	final static int defaultAlignmentSize = 4 * 1024; // 4KB
	final static int defaultHugePageLimit = 0; // no huge pages by default

	private HashMap<IbvPd, PdMemPool> pdMemPool; // One buddy allocator per protection domain
	private List<IbvMr> mrs;
	private boolean isOpen;

	long currentRegion = 0;
	private File dir;
	protected int access; // RDMA access flags to use when registering memory regions
	private long allocatedHugePageMemory;

	// Configurable values
	String hugePagePath = null;
	protected int allocationSize;
	protected int minAllocationSize;
	protected int alignmentSize;
	protected int hugePageLimit;




	public DaRPCMemPoolImplBuddy() throws IOException {
		isOpen = false;
		this.allocationSize = defaultAllocationSize;
		this.minAllocationSize = defaultMinAllocationSize;
		this.alignmentSize = defaultAlignmentSize;
		this.hugePageLimit = defaultHugePageLimit;

		init();
	}

	public DaRPCMemPoolImplBuddy(String hugePagePath, int hugePageLimit, int allocationSize, int minAllocationSize, int alignmentSize) throws IOException {
		isOpen = false;
		this.hugePagePath = hugePagePath;
		this.allocationSize = allocationSize;
		this.minAllocationSize = minAllocationSize;
		this.alignmentSize = alignmentSize;
		this.hugePageLimit = hugePageLimit;

		init();
	}

	protected void init() throws IOException {
		allocatedHugePageMemory = 0;
		this.access = IbvMr.IBV_ACCESS_LOCAL_WRITE | IbvMr.IBV_ACCESS_REMOTE_WRITE | IbvMr.IBV_ACCESS_REMOTE_READ;

		if ((this.hugePageLimit > 0) && (hugePagePath == null)) {
			throw new IOException("hugePageLimit is > 0 (" + this.hugePageLimit + "), but no hugepage path given.");
		}

		if (hugePagePath != null) {
			dir = new File(hugePagePath);
			if (!dir.exists()){
				dir.mkdirs();
			}
			for (File child : dir.listFiles()) {
				child.delete();
			}
		}

		pdMemPool = new HashMap<IbvPd, PdMemPool>();
		mrs = new ArrayList<IbvMr>();
		isOpen = true;
	}


// public API
	@Override
	public void close() throws IOException {
		cleanup();
	}
	@Override
	public ByteBuffer getBuffer(RdmaEndpoint endpoint, int size) throws IOException {
		return getBufferImpl(endpoint, size);
	}
	@Override
	public void freeBuffer(RdmaEndpoint endpoint, ByteBuffer b) {
		freeBufferImpl(endpoint, b);
	}
	@Override
	public int getLKey(RdmaEndpoint endpoint, ByteBuffer b) throws IllegalArgumentException {
		if (b == null) {
			System.out.println("getLKey(): Argument buffer is null. Cannot return lkey.");
			throw new IllegalArgumentException("getLKey(): Argument buffer is null. Cannot return lkey.");
		}
		PdMemPool pdm = pdMemPool.get(endpoint.getPd());
		BuddyInfo bi = pdm.usedBuddies.get(MemoryUtils.getAddress(b));
		if (bi != null) {
			return bi.lkey;
		} else {
			System.out.println("getLKey(): This buffer is not allocated. Cannot return lkey.");
			throw new IllegalArgumentException("getLKey(): This buffer is not allocated. Cannot return lkey.");
		}
	}




	synchronized void cleanup() {
		if (isOpen) {
			isOpen = false;
			pdMemPool = null;
			for (IbvMr m : mrs) {
				try {
					m.deregMr().execute().free();
				} catch (IOException e) {
					System.out.println("Could not unregister memory region.");
					e.printStackTrace();
				}
			}
			mrs = null;
		}

		// If hugepages were used, clean and delete the created files and the directory.
		if (hugePageLimit > 0) {
			if (dir.exists()) {
				for (File child : dir.listFiles()) {
					child.delete();
				}
				dir.delete();
			}
		}
	}

	@Override
	public void finalize() {
		// Just in case the user did not do that.
		try {
			close();
		} catch (Exception e) {
			System.out.println("MemoryPoolImplBuddy: Could not finalize() the memory pool");
			e.printStackTrace();
		}
	}


	// the next two methods allocate a buffer from the OS. The first one
	// allocates from the regular heap and the second one from huge pages.
	// These are two alternatives. If hugepages is configured by the user,
	// memory will first be allocated from huge pages and after reacing the limit,
	// more memory will be allocated from the regular heap.

	// allocate a buffer from the regular heap
	ByteBuffer allocateHeapBuffer() {
		ByteBuffer byteBuffer;

		if (alignmentSize > 1) {
			ByteBuffer rawBuffer = ByteBuffer.allocateDirect(allocationSize + alignmentSize);
			long rawBufferAddress = MemoryUtils.getAddress(rawBuffer);
			long alignmentOffset = rawBufferAddress % alignmentSize;
			if (alignmentOffset != 0) {
				rawBuffer.position(alignmentSize - (int)alignmentOffset);
			}
			byteBuffer = rawBuffer.slice();

		} else {
			byteBuffer = ByteBuffer.allocateDirect(allocationSize);
		}
		return (byteBuffer);
	}

	// allocate a buffer from hugepages
	ByteBuffer allocateHugePageBuffer() throws IOException {
		String path = hugePagePath + "/" + currentRegion++ + ".mem";
		RandomAccessFile randomFile = null;
		try {
			randomFile = new RandomAccessFile(path, "rw");
		} catch (FileNotFoundException e) {
			System.out.println("Path " + path + " to huge page directory not found.");
			throw e;
		}
		try {
			randomFile.setLength(allocationSize + alignmentSize);
		} catch (IOException e) {
			System.out.println("Coult not set allocation length of mapped random access file on huge page directory.");
			randomFile.close();
			throw e;
		}
		FileChannel channel = randomFile.getChannel();
		MappedByteBuffer mappedBuffer = null;
		try {
			mappedBuffer = channel.map(MapMode.READ_WRITE, 0,
					allocationSize);
		} catch (IOException e) {
			System.out.println("Could not map the huge page file on path " + path);
			randomFile.close();
			throw e;
		}
		randomFile.close();
		allocatedHugePageMemory += (allocationSize + alignmentSize);

		long rawBufferAddress = MemoryUtils.getAddress(mappedBuffer);
		long alignmentOffset = rawBufferAddress % alignmentSize;
		if (alignmentOffset != 0) {
			mappedBuffer.position(alignmentSize - (int)alignmentOffset);
		}
		ByteBuffer b = mappedBuffer.slice();
		return (b);
	}


	// Add a new chunk and register it with the IB device.
	// This adds a new "root" to the buddy tree.
	protected void addNewBuddy(PdMemPool pdm) throws IOException {
		BuddyInfo bi = new BuddyInfo();

		if ((allocatedHugePageMemory + allocationSize) < hugePageLimit) {
			bi.buffer = allocateHugePageBuffer();
		} else {
			bi.buffer = allocateHeapBuffer();
		}
		// Register buffer with IB card
		IbvMr mr = pdm.pd.regMr(bi.buffer, access).execute().free().getMr();
		mrs.add(mr);

		bi.s = state.FREE;
		bi.size = allocationSize;
		bi.parent = null;
		bi.sibling = null;
		bi.lkey = mr.getLkey();

		if (pdm.freeBuddies.get(allocationSize) == null) {
			pdm.freeBuddies.put(allocationSize, new LinkedList<BuddyInfo>());
		}
		pdm.freeBuddies.get(allocationSize).add(bi);
	}

	protected boolean split(PdMemPool pdm, int size) {
		if (size > allocationSize) {
			return false;
		}
		if (!pdm.freeBuddies.containsKey(size)) {
			if (!split(pdm, size << 1)) {
				// no free buddy, which could be split
				return false;
			}
		}
		LinkedList<BuddyInfo> l = pdm.freeBuddies.get(size);
		if (l == null) {
			return false;
		}
		BuddyInfo bi = l.removeFirst();
		if (l.size() == 0) {
			pdm.freeBuddies.remove(size);
		}
		bi.s = state.SPLIT;
		bi.buffer.position(0);
		bi.buffer.limit(size >> 1);
		ByteBuffer b1 = bi.buffer.slice();
		bi.buffer.position(size >> 1);
		bi.buffer.limit(size);
		ByteBuffer b2 = bi.buffer.slice();

		BuddyInfo bi1 = new BuddyInfo();
		BuddyInfo bi2 = new BuddyInfo();
		bi1.buffer = b1;
		bi1.s = state.FREE;
		bi1.size = (size >> 1);
		bi1.parent = bi;
		bi1.sibling = bi2;
		bi1.lkey = bi.lkey;

		bi2.buffer = b2;
		bi2.s = state.FREE;
		bi2.size = (size >> 1);
		bi2.parent = bi;
		bi2.sibling = bi1;
		bi2.lkey = bi.lkey;

		if (pdm.freeBuddies.get(size >> 1) == null) {
			pdm.freeBuddies.put(size >> 1, new LinkedList<BuddyInfo>());
		}
		pdm.freeBuddies.get(size >> 1).add(bi1);
		pdm.freeBuddies.get(size >> 1).add(bi2);

		return true;
	}


	protected ByteBuffer getPower2Buffer(PdMemPool pdm, int size) {
		if (!pdm.freeBuddies.containsKey(size)) {
			if (!split(pdm, size << 1)) {
				// no free buddy, which could be split
				return null;
			}
		}
		LinkedList<BuddyInfo> l = pdm.freeBuddies.get(size);
		if (l == null) {
			return null;
		}
		BuddyInfo bi = l.removeFirst();
		if (l.size() == 0) {
			pdm.freeBuddies.remove(size);
		}
		bi.s = state.USED;
		pdm.usedBuddies.put(MemoryUtils.getAddress(bi.buffer), bi);
		return bi.buffer;
	}

	synchronized ByteBuffer getBufferImpl(RdmaEndpoint endpoint, int size) throws IOException {
		int i = minAllocationSize;

		if (!pdMemPool.containsKey(endpoint.getPd())) {
			pdMemPool.put(endpoint.getPd(), new PdMemPool(endpoint.getPd()));
		}
		PdMemPool pdm = pdMemPool.get(endpoint.getPd());

		while(size > i) {
			i <<= 1;
		}

		ByteBuffer b = getPower2Buffer(pdm, i);
		if (b == null) {
			addNewBuddy(pdm);
			b = getPower2Buffer(pdm, i);
		}
		b.clear();
		return (b);
	}

	protected void merge(PdMemPool pdm, BuddyInfo bi) {
		if (bi.sibling != null) {
			if (bi.sibling.s == state.FREE) {
				BuddyInfo parent = bi.parent;
				parent.s = state.FREE;
				if (pdm.freeBuddies.get(parent.size) == null) {
					pdm.freeBuddies.put(parent.size, new LinkedList<BuddyInfo>());
				}
				pdm.freeBuddies.get(parent.size).add(parent);
				pdm.freeBuddies.get(bi.size).remove(bi.sibling);
				pdm.freeBuddies.get(bi.size).remove(bi);
				if (pdm.freeBuddies.get(bi.size).size() == 0) {
					pdm.freeBuddies.remove(bi.size);
				}
				merge(pdm, parent);
			}
		}
	}
	synchronized void freeBufferImpl(RdmaEndpoint endpoint, ByteBuffer b) {
		if (b == null) {
			return;
		}
		PdMemPool pdm = pdMemPool.get(endpoint.getPd());
		BuddyInfo bi = pdm.usedBuddies.remove(MemoryUtils.getAddress(b));
		// Buffer is not in the used list. Cannot free.
		if (bi == null) {
			return;
		}
		bi.s = state.FREE;
		if (pdm.freeBuddies.get(bi.size) == null) {
			pdm.freeBuddies.put(bi.size, new LinkedList<BuddyInfo>());
		}
		pdm.freeBuddies.get(bi.size).add(bi);
		merge(pdm, bi);
	}

	void setAllocationSize(int size) {
		allocationSize = size;
	}

	void setMinAllocationSize(int size) {
		minAllocationSize = size;
	}

	void setAlignment(int size) {
		alignmentSize = size;
	}

	int getAllocationSize() {
		return allocationSize;
	}

	int getMinAllocationSize(int size) {
		return minAllocationSize;
	}

	int getAlignment(int size) {
		return alignmentSize;
	}

	void printBuddies() {
		System.out.println("Free buddies:\n============");
		for (Iterator<PdMemPool> itpd = pdMemPool.values().iterator(); itpd.hasNext(); ) {
			PdMemPool pdm = itpd.next();
			for (Iterator<Integer> it = pdm.freeBuddies.keySet().iterator(); it.hasNext(); ) {
				Integer size = it.next();
				System.out.println("Size: " + size);
				LinkedList<BuddyInfo> l = pdm.freeBuddies.get(size);
				if (l != null) {
					for (Iterator<BuddyInfo> it2 = l.iterator(); it2.hasNext(); ) {
						BuddyInfo bi = it2.next();
						System.out.println(bi);
					}
				}
			}
		}
		System.out.println("============\n");

		System.out.println("Used buddies:\n============");
		for (Iterator<PdMemPool> itpd = pdMemPool.values().iterator(); itpd.hasNext(); ) {
			PdMemPool pdm = itpd.next();
			for (Iterator<BuddyInfo> it = pdm.usedBuddies.values().iterator(); it.hasNext(); ) {
				System.out.println(it.next());
			}
		}
		System.out.println("============\n");
	}



	// Internally used
	enum state {
		FREE,
		USED,
		SPLIT
	}

	// Internally used to keep track of buffer state
	class BuddyInfo {
		ByteBuffer buffer;
		BuddyInfo parent;
		BuddyInfo sibling;
		state s;
		int size;
		int lkey;
		@Override
		public String toString() {
			return new String("Size= " + size + ", state = "
					+ (s == state.FREE ? "FREE": s == state.USED ? "USED" : "SPLIT")
					+ ", address = " + MemoryUtils.getAddress(buffer)
					+ ", capacity = " + buffer.capacity()
					+ ", lkey = " + lkey);
		}
	}

	class PdMemPool {
		HashMap<Integer, LinkedList<BuddyInfo>> freeBuddies;
		HashMap<Long, BuddyInfo> usedBuddies;
		IbvPd pd;

		PdMemPool(IbvPd pd) {
			this.pd = pd;
			freeBuddies = new HashMap<Integer, LinkedList<BuddyInfo>>();
			usedBuddies = new HashMap<Long, BuddyInfo>();
		}
	}
}
