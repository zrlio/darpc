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

import com.ibm.disni.verbs.*;

public class RpcActiveClientEndpoint<R extends RdmaRpcMessage, T extends RdmaRpcMessage> extends RpcClientEndpoint<R,T> {
	private int streamCount;
	
	public RpcActiveClientEndpoint(RpcEndpointGroup<R, T> endpointGroup,
			RdmaCmId idPriv) throws IOException {
		super(endpointGroup, idPriv);
		this.streamCount = 1;
	}

	@Override
	public RpcStream<R, T> createStream() throws IOException {
		int streamId = this.streamCount;
		RpcStream<R,T> stream = new RpcStreamActive<R,T>(this, streamId);
		streamCount++;
		return stream;
	}

	@Override
	public SVCPostSend getSendSlot(ArrayBlockingQueue<SVCPostSend> freePostSend) throws IOException {
		try {
			return freePostSend.take();
		} catch(Exception e){
			throw new IOException(e);
		}
	}

}
