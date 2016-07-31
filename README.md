# DaRPC: Data Center Remote Procedure Call

DaRPC is a Java library that provides ultra-low latency RPC for RDMA capable network interfaces. 
The unique features of DaRPC include:

* Ultra-low RPC latencies very close to the network roundtrip latencies.
* Efficient asynchronous non-blocking RPC interfaces
* Fine grained control over network trade-offs (interrups vs polling)

## Building DaRPC

DaRPC is built using [Apache Maven](http://maven.apache.org/).
To build DaRPC and its example programs, execute the following steps:

1. Obtain a copy of DaRPC from [Github](https://github.com/zrlio/darpc)
2. Make sure your local maven repo contains DiSNI, if not build DiSNI from [Github](https://github.com/zrlio/disni)
2. Compile and package the source using: mvn -DskipTests install

## How to Run a Simple Example

1. After building DaRPC, make sure DaRPC and its dependencies are in the classpath (e.g., darpc-1.0-jar-with-dependencies.jar). Also add the DaRPC test jar (darpc-1.0-tests.jar) which includes the examples.
2. Make sure libdisni and libaffinity are both part of the LD_LIBRARY_PATH
3. Make sure the RDMA network interface is configured and up on the test machines (run ibv\_devices to see the list of RDMA NICs). If your machine does not have RDMA hardware, you can also use SoftiWARP from [Github](https://github.com/zrlio/softiwarp). 
4. Run the server\: java com.ibm.darpc.examples.server.DaRPCServer -a \<server IP\>
5. Run the client\: java com.ibm.darpc.examples.client.DaRPCClient -a \<server IP\> 
