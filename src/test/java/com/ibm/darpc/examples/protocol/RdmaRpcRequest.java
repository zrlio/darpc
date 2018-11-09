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

package com.ibm.darpc.examples.protocol;

import java.nio.ByteBuffer;

import com.ibm.darpc.DaRPCMessage;

public class RdmaRpcRequest implements DaRPCMessage {
	public static int SERIALIZED_SIZE = 16;
	
	private int cmd;
	private int param;
	private long time;
	
	public RdmaRpcRequest(){
	}

	public int size() {
		return SERIALIZED_SIZE;
	}

	@Override
	public void update(ByteBuffer buffer) {
		cmd = buffer.getInt();
		param = buffer.getInt();
		time = buffer.getLong();
	}

	@Override
	public int write(ByteBuffer buffer) {
		buffer.putInt(cmd);
		buffer.putInt(param);
		buffer.putLong(time);
		
		return SERIALIZED_SIZE;
	}

	public int getCmd() {
		return cmd;
	}

	public void setCmd(int cmd) {
		this.cmd = cmd;
	}

	public int getParam() {
		return param;
	}

	public void setParam(int param) {
		this.param = param;
	}
}