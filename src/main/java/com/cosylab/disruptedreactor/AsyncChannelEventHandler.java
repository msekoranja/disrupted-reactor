package com.cosylab.disruptedreactor;

import java.io.IOException;
import java.nio.channels.SelectionKey;

import com.cosylab.disruptedreactor.reactor.DisruptedReactor;

// must throw (pass/re-throw) an IOException when channel is closed 
public interface AsyncChannelEventHandler {
	static final int CHANNEL_CLOSED = 0xFFFFFFFF;
	DisruptedReactor getReactor();
	default int processRead(SelectionKey key) throws IOException { return 0; }
	default int processWrite(SelectionKey key) throws IOException { return 0; }
	default int processAccept(SelectionKey key) throws IOException { return 0; }
	default int processConnect(SelectionKey key) throws IOException { return 0; }
}