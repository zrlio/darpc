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
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ibm.disni.util.*;

public class DaRPCResourceManager {
	private static final Logger logger = LoggerFactory.getLogger("com.ibm.darpc");
	private RpcResourceAllocator[] allocaters;
	
	public DaRPCResourceManager(long[] affinities, int timeout) throws Exception {
		this.allocaters = new RpcResourceAllocator[affinities.length];
		for (int i = 0; i < affinities.length; i++){
			allocaters[i] = new RpcResourceAllocator(affinities[i], i, timeout);
			allocaters[i].start();
		}
	}
	
	public void allocateResources(DaRPCServerEndpoint<?,?> endpoint) throws Exception {
		logger.info("dispatching resource, clusterid " + endpoint.clusterId());
		allocaters[endpoint.clusterId()].initResource(endpoint);
	}
	
	public void close() throws IOException, InterruptedException {
		for (int i = 0; i < allocaters.length; i++){
			allocaters[i].close();
		}		
	}

	public static class RpcResourceAllocator implements Runnable {
		private static Logger logger = LoggerFactory.getLogger("com.ibm.zac.darpc");
		private LinkedBlockingQueue<DaRPCEndpoint<?,?>> requestQueue;
		private long affinity;
		private int index;
		private boolean running;
		private Thread thread;
		private int timeout;
		
		public RpcResourceAllocator(long affinity, int index, int timeout) throws Exception {
			this.affinity = affinity;
			this.index = index;
			this.requestQueue = new LinkedBlockingQueue<DaRPCEndpoint<?,?>>();
			this.running = false;
			this.timeout = timeout;
			if (timeout <= 0){
				this.timeout = Integer.MAX_VALUE;
			}
			this.thread = new Thread(this);
		}
		
		public void initResource(DaRPCEndpoint<?,?> endpoint) {
			requestQueue.add(endpoint);
		}	
		
		public synchronized void start(){
			running = true;
			thread.start();
		}		
		
		public void run() {
			NativeAffinity.setAffinity(affinity);
			logger.info("running resource management, index " + index + ", affinity " + affinity + ", timeout " + timeout);
			while(running){
				try {
					DaRPCEndpoint<?,?> endpoint = requestQueue.poll(timeout, TimeUnit.MILLISECONDS);
					if (endpoint != null){
						logger.info("allocating resources, cluster " + index + ", endpoint " + endpoint.getEndpointId());
						endpoint.allocateResources();
					}
				} catch(Exception e){
					e.printStackTrace();
				}
			}
		}
		
		public void close() throws IOException, InterruptedException {
			running = false;
			thread.join();
			logger.info("resource management closed");
		}		
	}


}
