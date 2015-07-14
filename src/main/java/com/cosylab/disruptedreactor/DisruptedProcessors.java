package com.cosylab.disruptedreactor;

import java.util.concurrent.Executor;

import com.lmax.disruptor.SleepingWaitStrategy;
import com.lmax.disruptor.WaitStrategy;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;

public final class DisruptedProcessors {

	final static WaitStrategy waitStrategy = new SleepingWaitStrategy(100);	// TODO made configurable

	public static Disruptor<SelectionEvent>[] createProcessors(Executor executor, int processors, int maxNumberOfChannels)
	{
		// multiply by 2 since 2 SelectionEvents (read and write) can get into the queue per channel
		final int ringSize = nearestPowerOf2((maxNumberOfChannels*2)/processors + 1);

		@SuppressWarnings("unchecked")
		final Disruptor<SelectionEvent>[] disruptors = new Disruptor[processors];
		for (int i = 0; i < processors; i++)
		{
			disruptors[i] = createProcessorDisruptor(ringSize, executor);
			disruptors[i].start();
		}

		return disruptors;
	}
	
	@SuppressWarnings("unchecked")
	public static Disruptor<SelectionEvent> createProcessorDisruptor(int ringSize, Executor executor)
	{

		final Disruptor<SelectionEvent> disruptor =
				new Disruptor<>(
						SelectionEvent::new,
						ringSize,
						executor,
						ProducerType.SINGLE,
						waitStrategy);

		disruptor.handleEventsWith(DisruptedAsyncChannelEventHandler::onSelectionEvent);
		
		return disruptor;
	}
	
	public static final int nearestPowerOf2(int v) {
		if (v < 0)
			throw new IllegalArgumentException("v < 0");

		v--;
		v |= v >> 1;
		v |= v >> 2;
		v |= v >> 4;
		v |= v >> 8;
		v |= v >> 16;
		v++;		
		
		return v;
	}

}
