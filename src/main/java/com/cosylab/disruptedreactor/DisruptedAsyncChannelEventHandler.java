/**
 * 
 */
package com.cosylab.disruptedreactor;

import java.io.IOException;
import java.nio.channels.SelectableChannel;
import java.nio.channels.SelectionKey;
import java.util.concurrent.atomic.AtomicBoolean;

import com.cosylab.disruptedreactor.reactor.DisruptedReactor;
import com.cosylab.disruptedreactor.reactor.SelectionHandler;
import com.lmax.disruptor.RingBuffer;

public abstract class DisruptedAsyncChannelEventHandler
	implements AsyncChannelEventHandler, SelectionHandler {

	protected final DisruptedReactor reactor;
	protected final SelectableChannel channel;
	protected final RingBuffer<SelectionEvent> ringBuffer;
	
	// OP_WRITE is always latching
	protected final int defaultInterestOp;

	/**
	 * @param reactor
	 * @param channel
	 * @param ringBuffer
	 */
	public DisruptedAsyncChannelEventHandler(
			DisruptedReactor reactor, 
			SelectableChannel channel,
			int defaultInterestOp,
			RingBuffer<SelectionEvent> ringBuffer)
	{
		this.reactor = reactor;
		this.channel = channel;
		this.defaultInterestOp = defaultInterestOp;
		this.ringBuffer = ringBuffer;
	}

	@Override
	public DisruptedReactor getReactor() {
		return reactor;
	}
	
	protected final AtomicBoolean writeOp = new AtomicBoolean();
	public void latchWriteOp() {
		if (!writeOp.getAndSet(true))
			reactor.interestOps(channel, defaultInterestOp | SelectionKey.OP_WRITE);
	}
	
	@Override
	public void selected(SelectionKey key) {
		key.interestOps(0);

		final long seq = ringBuffer.next();
		final SelectionEvent event = ringBuffer.get(seq);
		event.key = key;
		event.readyOps = key.readyOps();
System.out.println("selected: " + event.readyOps);
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
		final int readyOps = event.readyOps;
System.out.println("readyOps: " + readyOps);
		//		final AsyncChannelEventHandler handler = (AsyncChannelEventHandler)key.attachment();
		final DisruptedAsyncChannelEventHandler handler = (DisruptedAsyncChannelEventHandler)key.attachment();
		
		try
		{
			int ops = handler.defaultInterestOp;
			
			if ((readyOps & SelectionKey.OP_READ) != 0)
				ops |= handler.processRead(key);
		
			// close (on 'channel.read() < 0') without throwing an exception
			if (ops == AsyncChannelEventHandler.CHANNEL_CLOSED)
			{
				close(handler, key);
				return;
			}
			
			if ((readyOps & SelectionKey.OP_WRITE) != 0 ||
				(ops & SelectionKey.OP_WRITE) != 0)			// low latency (immediate write after read)
			{
				handler.writeOp.set(false);
				ops ^= SelectionKey.OP_WRITE; 	// clear OP_WRITE since we are handling it
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
	
			int writeOp = ops & SelectionKey.OP_WRITE;
			if (writeOp != 0)
				handler.writeOp.set(true);

			handler.getReactor().interestOps(key, handler.defaultInterestOp | writeOp);
		}
		catch (IOException ex)
		{
			close(handler, key);
		}
	}
}
