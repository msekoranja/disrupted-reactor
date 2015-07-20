package com.cosylab.disruptedreactor.reactor;

import java.io.IOException;
import java.nio.channels.CancelledKeyException;
import java.nio.channels.SelectableChannel;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

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
	final ExecutorService executor;
	final Disruptor<ReactorRequest> disruptor;
	final RingBuffer<ReactorRequest> ringBuffer;
	
	final Selector selector;
	
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
		selector.close();
		
		//disruptor.shutdown(timeout, timeUnit);
		disruptor.halt();
		//executor.shutdown();
		//executor.awaitTermination(timeout, unit);
		executor.shutdownNow();
	}

	public void register(SelectableChannel channel, int ops, SelectionHandler handler)
	{
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
		final long seq = ringBuffer.next();
		final ReactorRequest event = ringBuffer.get(seq);
		event.type = RequestType.INTEREST_OPS;
		event.key = key;
		event.ops = ops;
		ringBuffer.publish(seq);
		
		selector.wakeup();
	}
	
	// note: onClose() not called by this method
	public void unregisterAndClose(SelectionKey key)
	{
		final long seq = ringBuffer.next();
		final ReactorRequest event = ringBuffer.get(seq);
		event.type = RequestType.CLOSE;
		event.key = key;
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
				event.key.interestOps(event.ops);
				break;
			case REGISTER:
				event.channel.register(selector, event.ops, event.handler);
				break;
			case UNREGISTER:
				event.key.cancel();
				break;
			case CLOSE:
				// implicit close of a SelectionKey
				event.key.channel().close();
				break;
			}
		} catch (Throwable th) {
			// guard
			// TODO
			th.printStackTrace();
		}

		event.clear();
	}	
	
	private void doSelect(boolean block)
	{
		try
		{
			int selectedCount = block ? selector.select() : selector.selectNow();
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
