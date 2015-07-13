package com.cosylab.disruptedreactor;

import java.nio.channels.SelectionKey;

import com.cosylab.disruptedreactor.reactor.DisruptedReactor;

public interface AsyncChannelEventHandler {
	DisruptedReactor getReactor();
	default int processRead(SelectionKey key) { return 0; }
	default int processWrite(SelectionKey key) { return 0; }
	default int processAccept(SelectionKey key) { return 0; }
	default int processConnect(SelectionKey key) { return 0; }
}