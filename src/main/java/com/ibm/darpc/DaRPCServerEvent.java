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

public class DaRPCServerEvent<R extends DaRPCMessage, T extends DaRPCMessage> {
	private DaRPCServerEndpoint<R,T> endpoint;
	private R request;
	private T response;
	private int ticket;
	
	DaRPCServerEvent(DaRPCServerEndpoint<R,T> endpoint, R request, T response){
		this.endpoint = endpoint;
		this.request = request;
		this.response = response;
		this.ticket = 0;
	}
	
	public R getReceiveMessage() {
		return request;
	}

	public T getSendMessage() {
		return response;
	}
	
	public void triggerResponse() throws IOException {
		endpoint.sendResponse(this);
	}
	
	public int getTicket(){
		return ticket;
	}
	
	void stamp(int ticket){
		this.ticket = ticket;
	}
	

}
