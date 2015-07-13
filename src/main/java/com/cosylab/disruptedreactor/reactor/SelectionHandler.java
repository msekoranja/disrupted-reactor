package com.cosylab.disruptedreactor.reactor;

import java.nio.channels.SelectionKey;

public interface SelectionHandler {
	void selected(SelectionKey key);
}