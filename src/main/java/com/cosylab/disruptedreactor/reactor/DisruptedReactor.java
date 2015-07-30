package com.cosylab.disruptedreactor.reactor;

import java.io.IOException;
import java.nio.channels.CancelledKeyException;
import java.nio.channels.SelectableChannel;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

import com.cosylab.disruptedreactor.reactor.DisruptedReactor.ReactorRequest.RequestType;
import com.lmax.disruptor.AlertException;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.Sequence;
import com.lmax.disruptor.SequenceBarrier;
import com.lmax.disruptor.WaitStrategy;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;

public final class DisruptedReactor implements AutoCloseable
{
	private final ExecutorService executor;
	private final Disruptor<ReactorRequest> disruptor;
	private final RingBuffer<ReactorRequest> ringBuffer;
	
	private final Selector selector;
	
	private final AtomicBoolean closed = new AtomicBoolean();
	
	@SuppressWarnings("unchecked")
	public DisruptedReactor(int ringSize) throws IOException
	{
		selector = Selector.open();
		
		executor = Executors.newSingleThreadExecutor();
		disruptor = new Disruptor<>(
						ReactorRequest::new,
						ringSize,
						executor,
						ProducerType.SINGLE,
						new SelectorWaitStrategy());
		disruptor.handleEventsWith((event, sequence, endOfBatch) -> handleRequest(event));
		ringBuffer = disruptor.start();
	}
	
	@Override
	public void close() throws IOException
	{
		if (closed.getAndSet(true))
			return;
		
		selector.close();
		
		//disruptor.shutdown(timeout, timeUnit);
		disruptor.halt();
		//executor.shutdown();
		//executor.awaitTermination(timeout, unit);
		executor.shutdownNow();
	}

	public void register(SelectableChannel channel, int ops, SelectionHandler handler)
	{
		System.out.println("register(SelectableChannel channel, int ops, SelectionHandler handler): " + ops);
	Thread.dumpStack();
		final long seq = ringBuffer.next();
		final ReactorRequest event = ringBuffer.get(seq);
		event.type = RequestType.REGISTER;
		event.channel = channel;
		event.ops = ops;
		event.handler = handler;
		ringBuffer.publish(seq);
		
		selector.wakeup();
	}
	
	public void unregsiter(SelectionKey key)
	{
		final long seq = ringBuffer.next();
		final ReactorRequest event = ringBuffer.get(seq);
		event.type = RequestType.UNREGISTER;
		event.key = key;
		ringBuffer.publish(seq);
		
		selector.wakeup();
	}

	public void interestOps(SelectionKey key, int ops)
	{
		System.out.println("interestOps(SelectionKey key, int ops): " + ops);
		final long seq = ringBuffer.next();
		final ReactorRequest event = ringBuffer.get(seq);
		event.type = RequestType.INTEREST_OPS;
		event.key = key;
		event.ops = ops;
		ringBuffer.publish(seq);
		
		selector.wakeup();
	}
	
	public void interestOps(SelectableChannel channel, int ops)
	{
		System.out.println("interestOps(SelectableChannel channel, int ops): " + ops);
		if (ops == 5) Thread.dumpStack();
		final long seq = ringBuffer.next();
		final ReactorRequest event = ringBuffer.get(seq);
		event.type = RequestType.INTEREST_OPS;
		event.channel = channel; if (channel == null) System.out.println("channel == null !!!");
		event.ops = ops;
		ringBuffer.publish(seq);
		
		selector.wakeup();
	}

	// note: onClose() not called by this method
	public void unregisterAndClose(SelectionKey key)
	{
		System.out.println("unregisterAndClose(SelectionKey key)");
		final long seq = ringBuffer.next();
		final ReactorRequest event = ringBuffer.get(seq);
		event.type = RequestType.CLOSE;
		event.key = key;
		ringBuffer.publish(seq);
		
		selector.wakeup();
	}

	public void unregisterAndClose(SelectableChannel channel)
	{
		System.out.println("unregisterAndClose(SelectableChannel channel)");
		final long seq = ringBuffer.next();
		final ReactorRequest event = ringBuffer.get(seq);
		event.type = RequestType.CLOSE;
		event.channel = channel;
		ringBuffer.publish(seq);
		
		selector.wakeup();
	}
	
	private void handleRequest(ReactorRequest event)
	{
		System.out.println("onEvent: " + event.type);
		try
		{
			switch (event.type)
			{
			case INTEREST_OPS:
				if (event.key == null)
					event.key = event.channel.keyFor(selector);
if (event.key != null)
				event.key.interestOps(event.ops);
				break;
			case REGISTER:
				event.channel.register(selector, event.ops, event.handler);
				break;
			case UNREGISTER:
				event.key.cancel();
				break;
			case CLOSE:
				SelectableChannel channel = event.channel;
				if (channel == null)
					channel = event.key.channel();
				// implicit close of a SelectionKey
				channel.close();
				break;
			}
		} catch (NullPointerException npe) {
			npe.printStackTrace();
		} catch (Throwable th) {
			// guard
			// TODO
			th.printStackTrace();
		}

		event.clear();
	}	
	
	private void doSelect(boolean block)
	{
		if (block)		System.out.println("doSelect: " + block);
		
		try
		{
			int selectedCount = block ? selector.select() : selector.selectNow();
if (block)			System.out.println(selectedCount);
			if (selectedCount > 0)
			{
				Set<SelectionKey> selectedKeys = selector.selectedKeys();
				for (SelectionKey key : selectedKeys)
				{
					try
					{
						((SelectionHandler)key.attachment()).selected(key);
					}
					catch (CancelledKeyException cke) {
						// noop
					}
				}
				selectedKeys.clear();
			}
		} catch (Throwable th) {
			// TODO
			th.printStackTrace();
		}
	}
   
	
	static class ReactorRequest {
		enum RequestType { REGISTER, UNREGISTER, INTEREST_OPS, CLOSE };
		RequestType type;
		SelectableChannel channel;
		SelectionHandler handler;
		SelectionKey key;
		int ops;
		
		// reset event and let GC do its work
		public void clear()
		{
			channel = null;
			handler = null;
			key = null;
		}
	}
	
	class SelectorWaitStrategy implements WaitStrategy {
		// TODO configurable
		final int DEFAULT_RETRIES = 200;

		@Override
		public long waitFor(long sequence, Sequence cursorSequence, Sequence dependentSequence, SequenceBarrier barrier)
			throws AlertException, InterruptedException
		{
			int counter = DEFAULT_RETRIES;

			long availableSequence;
			while ((availableSequence = cursorSequence.get()) < sequence)
			{
				barrier.checkAlert();
				
				if (closed.get())
					break;

				if (counter > 0)
				{
					--counter;

					// non-blocking select
					doSelect(false);
				}
				else
				{
					// blocking select
					doSelect(true);
				}
			}

			while ((availableSequence = dependentSequence.get()) < sequence)
			{
				barrier.checkAlert();
			}

			return availableSequence;
		}

		@Override
		public void signalAllWhenBlocking()
		{
			selector.wakeup();
		}
	}
	
}
