/**
 * 
 */
package com.cosylab.disruptedreactor;

import java.io.IOException;
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
	
	private static final void close(AsyncChannelEventHandler handler, SelectionKey key) {
		try {
			handler.onClose(key);
		} catch (Throwable th) {
			// TODO
			th.printStackTrace();
		}

		handler.getReactor().unregisterAndClose(key);
	}
	
	public static final void onSelectionEvent(SelectionEvent event, long sequence, boolean endOfBatch)
	{
		final SelectionKey key = event.key;
		final int readyOps = key.readyOps();
		final AsyncChannelEventHandler handler = (AsyncChannelEventHandler)key.attachment();
		
		try
		{
			int ops = 0;
			
			if ((readyOps & SelectionKey.OP_READ) != 0)
				ops |= handler.processRead(key);
		
			// close (on 'channel.read() < 0') without throwing an exception
			if (ops == AsyncChannelEventHandler.CHANNEL_CLOSED)
			{
				close(handler, key);
				return;
			}
			
			if ((readyOps & SelectionKey.OP_WRITE) != 0)
				ops |= handler.processWrite(key);
			// TODO configurable low latency
			else if ((ops & SelectionKey.OP_WRITE) != 0)
			{
				ops ^= SelectionKey.OP_WRITE; 
				ops |= handler.processWrite(key);
			}
			
			// close (on 'channel.write() < 0') without throwing an exception
			if (ops == AsyncChannelEventHandler.CHANNEL_CLOSED)
			{
				close(handler, key);
				return;
			}

			if ((readyOps & SelectionKey.OP_ACCEPT) != 0)
				ops |= handler.processAccept(key);
	
			if ((readyOps & SelectionKey.OP_CONNECT) != 0)
				ops |= handler.processConnect(key);
	
			handler.getReactor().interestOps(key, ops);
		}
		catch (IOException ex)
		{
			close(handler, key);
		}
	}
}
