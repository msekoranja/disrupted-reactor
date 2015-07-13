/**
 * 
 */
package com.cosylab.disruptedreactor;

import java.nio.channels.SelectableChannel;
import java.nio.channels.SelectionKey;

import com.cosylab.disruptedreactor.reactor.DisruptedReactor;
import com.cosylab.disruptedreactor.reactor.SelectionHandler;
import com.lmax.disruptor.RingBuffer;

public abstract class DisruptedAsyncChannelEventHandler
	implements AsyncChannelEventHandler, SelectionHandler {

	protected final DisruptedReactor reactor;
	protected final SelectableChannel channel;
	protected final RingBuffer<SelectionEvent> ringBuffer;

	/**
	 * @param reactor
	 * @param channel
	 * @param ringBuffer
	 */
	public DisruptedAsyncChannelEventHandler(
			DisruptedReactor reactor, 
			SelectableChannel channel,
			RingBuffer<SelectionEvent> ringBuffer)
	{
		this.reactor = reactor;
		this.channel = channel;
		this.ringBuffer = ringBuffer;
	}

	@Override
	public DisruptedReactor getReactor() {
		return reactor;
	}
	
	@Override
	public void selected(SelectionKey key) {
		key.interestOps(0);

		final long seq = ringBuffer.next();
        final SelectionEvent event = ringBuffer.get(seq);
		event.key = key;
		ringBuffer.publish(seq);
	}
	
	public static final void onSelectionEvent(SelectionEvent event, long sequence, boolean endOfBatch)
    {
		final SelectionKey key = event.key;
		final int readyOps = key.readyOps();
        final AsyncChannelEventHandler handler = (AsyncChannelEventHandler)key.attachment();
        
        int ops = 0;
        
        if ((readyOps & SelectionKey.OP_READ) != 0)
        	ops |= handler.processRead(key);
        
        // read operation can close
        if (!key.isValid())
        	return;
        
        if ((readyOps & SelectionKey.OP_WRITE) != 0)
        	ops |= handler.processWrite(key);
        
        if ((readyOps & SelectionKey.OP_ACCEPT) != 0)
        	ops |= handler.processAccept(key);

        if ((readyOps & SelectionKey.OP_CONNECT) != 0)
        	ops |= handler.processConnect(key);

        handler.getReactor().interestOps(key, ops);
    }
}
