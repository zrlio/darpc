<!--
{% comment %}

Copyright (C) 2016-2018, IBM Corporation

Licensed to the Apache Software Foundation (ASF) under one or more
contributor license agreements.  See the NOTICE file distributed with
this work for additional information regarding copyright ownership.
The ASF licenses this file to You under the Apache License, Version 2.0
(the "License"); you may not use this file except in compliance with
the License.  You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
{% endcomment %}
-->

# DaRPC: Data Center Remote Procedure Call

DaRPC is a Java library that provides ultra-low latency RPC for RDMA capable network interfaces. 
The unique features of DaRPC include:

* Ultra-low RPC latencies close to the raw hardware roundtrip latencies. 
  (Depending on the networking hardware and the CPU intensity of the RPC call, RPC latencies of 5-6 microseconds are possible)
* Efficient asynchronous and non-blocking RPC interfaces
* Fine grained control over network trade-offs (interrupts, polling, CPU affinities, etc.)

## Building DaRPC

DaRPC is built using [Apache Maven](http://maven.apache.org/) and requires Java version 8 or higher.
To build DaRPC and its example programs, execute the following steps:

1. Obtain a copy of DaRPC from [Github](https://github.com/zrlio/darpc)
2. Make sure your local maven repo contains DiSNI, if not build DiSNI from [Github](https://github.com/zrlio/disni)
2. Compile and package the source using: mvn -DskipTests install

## How to Run a Simple Example

1. After building DaRPC, make sure DaRPC and its dependencies are in the classpath (e.g., darpc-1.9-jar-with-dependencies.jar). Also add the DaRPC test jar (darpc-1.0-tests.jar) which includes the examples.
2. Make sure libdisni is part of the LD_LIBRARY_PATH
3. Make sure the RDMA network interface is configured and up on the test machines (run ibv\_devices to see the list of RDMA NICs). If your machine does not have RDMA hardware, you can also use SoftiWARP from [Github](https://github.com/zrlio/softiwarp). 
4. Run the server\: java com.ibm.darpc.examples.server.DaRPCServer -a \<server IP\>
5. Run the client\: java com.ibm.darpc.examples.client.DaRPCClient -a \<server IP\> 

## Maven Integration

To use DaRPC in your maven application use the following snippet in your pom.xml file:

    <dependency>
      <groupId>com.ibm.darpc</groupId>
      <artifactId>darpc</artifactId>
      <version>1.9</version>
    </dependency>

## Basic Steps Required to Implement your own RPC Service

Define the RPC request and response messages

```
	public class RdmaRpcRequest implements RdmaRpcMessage {
		public static int SERIALIZED_SIZE = 16;
		
		private int cmd;
		private int param;
		private long time;
		
		public RdmaRpcRequest(){
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
	}
```
Define the protocol
```
	public class RdmaRpcProtocol extends RdmaRpcService<RdmaRpcRequest, RdmaRpcResponse> {
		@Override
		public RdmaRpcRequest createRequest() {
			return new RdmaRpcRequest();
		}

		@Override
		public RdmaRpcResponse createResponse() {
			return new RdmaRpcResponse();
		}
	}
```
Define the actual RPC service
```
	public class RdmaRpcService extends RdmaRpcProtocol {
		public void processServerEvent(RpcServerEvent<RdmaRpcRequest, RdmaRpcResponse> event) throws IOException {
			RdmaRpcProtocol.RdmaRpcRequest request = event.getRequest();
			RdmaRpcProtocol.RdmaRpcResponse response = event.getResponse();
			response.setName(request.getParam() + 1);
			event.triggerResponse();
		}
	}
```
Start the server
```
	RdmaRpcService rpcService = new RdmaRpcService();
	RpcActiveEndpointGroup<RdmaRpcRequest, RdmaRpcResponse> rpcGroup = RpcActiveEndpointGroup.createDefault(rpcService);
	RdmaServerEndpoint<RpcClientEndpoint<RdmaRpcRequest, RdmaRpcResponse>> rpcEndpoint = rpcGroup.createServerEndpoint();
	rpcEndpoint.bind(addr);
	while(true){
		serverEp.accept();
	}
```	
Call the RPC service from a client
```
	RpcEndpointGroup rpcGroup = RpcPassiveEndpointGroup.createDefault();
	RpcEndpoint rpcEndpoint = rpcGroup.createEndpoint();
	rpcEndpoint.connect(address);
	RpcStream<RdmaRpcRequest, RdmaRpcResponse> stream = rpcEndpoint.createStream();
	RdmaRpcRequest request = new RdmaRpcRequest();
	request.setParam(77);
	RdmaRpcResponse response = new RdmaRpcResponse();
	RpcFuture<RdmaRpcRequest, RdmaRpcResponse> future = stream.request(request, response);
	...
	response = future.get();
	System.out.println("RPC completed, return value " + response.getReturnValue()); //should print 78
```
## Choosing the RpcEndpointGroup 

RpcEndpointGroups are containers and factories for RPC connections (RpcEndpoint). There are two types of groups available and which type works best depends on the application. The RpcActiveEndpointGroup actively processes network events caused by RDMA messages being transmitted or received. The RpcPassiveEndpointGroup processes network events directly in the process context of the application. As such, the passive mode has typically lower latency but may suffer from contention to per-connection hardware resources in case of large numbers of threads. The active mode, on the other hand, is more robust under large numbers of threads, but has higher latencies. Applications can control the cores and the NUMA affinities that are used for event processing. For server-side RPC processing the active mode is always used. The DaRPC paper discusses the trade-offs between active and passive RPC processing in more detail. 

## Contributions

PRs are always welcome. Please fork, and make necessary modifications 
you propose, and let us know. 

## Contact 

If you have questions or suggestions, feel free to post at:

https://groups.google.com/forum/#!forum/zrlio-users

or email: zrlio-users@googlegroups.com
