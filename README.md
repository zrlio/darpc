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
2. Compile and package the source using: mvn -DskipTests install

