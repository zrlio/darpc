package com.ibm.darpc;

import java.io.IOException;

public interface RpcEvent<R,S> {
	public R getReceiveMessage();
	public S getSendMessage();
	int getTicket();
}
