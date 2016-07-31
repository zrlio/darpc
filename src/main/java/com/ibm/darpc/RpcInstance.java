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
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ibm.disni.verbs.*;

public class RpcInstance<R extends RdmaRpcMessage, T extends RdmaRpcMessage> {
	private static final Logger logger = LoggerFactory.getLogger("com.ibm.darpc");
	
	private RpcCluster<R,T>[] processorArray;
	protected ConcurrentHashMap<RdmaCmId, Object> ids;
	private int poolsize;
	private int cqSize;
	
	@SuppressWarnings("unchecked")
	public RpcInstance(IbvContext context, int cqSize, int wrSize, long[] affinities, int timeout, boolean polling) throws IOException {
		this.poolsize = affinities.length;
		this.cqSize = cqSize;
		logger.info("new cq pool, size " + poolsize);
		processorArray = new RpcCluster[affinities.length];
		for (int i = 0; i < affinities.length; i++){
			processorArray[i] = new RpcCluster<R,T>(context, cqSize, wrSize, affinities[i], i, timeout, polling);
			processorArray[i].start();
		}
		this.ids = new ConcurrentHashMap<RdmaCmId, Object>();
	}
	
	
	public RpcCluster<R,T> getProcessor(int clusterId) {
		return processorArray[clusterId];
	}
	
	public void close() throws IOException, InterruptedException {
		for (int i = 0; i < processorArray.length; i++){
			processorArray[i].close();
		}
	}	
}
