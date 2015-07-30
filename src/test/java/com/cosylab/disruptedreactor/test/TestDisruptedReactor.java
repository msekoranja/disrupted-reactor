package com.cosylab.disruptedreactor.test;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.StandardSocketOptions;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.SelectableChannel;
import java.nio.channels.SelectionKey;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.nio.channels.WritableByteChannel;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

import com.cosylab.disruptedreactor.AsyncChannelEventHandler;
import com.cosylab.disruptedreactor.DisruptedAsyncChannelEventHandler;
import com.cosylab.disruptedreactor.DisruptedProcessors;
import com.cosylab.disruptedreactor.SelectionEvent;
import com.cosylab.disruptedreactor.reactor.DisruptedReactor;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.dsl.Disruptor;

public class TestDisruptedReactor {
	
	
	public static void main(String[] args) throws IOException
	{
		final int processors = Runtime.getRuntime().availableProcessors() - 1;
		final int maxNumberOfChannels = 2048;

		final Executor executor = Executors.newFixedThreadPool(processors);		
		final Disruptor<SelectionEvent>[] disruptors = DisruptedProcessors.createProcessors(executor, processors, maxNumberOfChannels);
		final DisruptedReactor reactor = new DisruptedReactor(DisruptedProcessors.nearestPowerOf2(maxNumberOfChannels*2));
		
		final ServerSocketChannel serverSocketChannel = ServerSocketChannel.open();
		serverSocketChannel.configureBlocking(false);
		serverSocketChannel.bind(new InetSocketAddress("127.0.0.1", 3000), 1024);
		
		reactor.register(serverSocketChannel, SelectionKey.OP_ACCEPT,
				new AcceptorHandler(reactor, serverSocketChannel, disruptors)
		);
		
		
	}
	
	static class AcceptorHandler extends DisruptedAsyncChannelEventHandler {
	
		private final Disruptor<SelectionEvent>[] disruptors;
		private int disruptorSelector = 0;
		
		AcceptorHandler(
				DisruptedReactor reactor,
				ServerSocketChannel channel,
				Disruptor<SelectionEvent>[] disruptors) {
			super(reactor, channel, SelectionKey.OP_ACCEPT, disruptors[0].getRingBuffer());
			this.disruptors = disruptors;
		}
	
	
		@Override
		public int processAccept(SelectionKey key) {
			System.out.println("processAccept");
	
			try {
				SocketChannel c = ((ServerSocketChannel)channel).accept();
				c.configureBlocking(false);
				c.setOption(StandardSocketOptions.SO_KEEPALIVE, true);
				c.setOption(StandardSocketOptions.TCP_NODELAY, true);
				
				int disruptorIndex = disruptorSelector++ % disruptors.length;
				reactor.register(c, SelectionKey.OP_READ,
						new TestReactorEventHandler(reactor, c, disruptors[disruptorIndex].getRingBuffer()));

			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
	
			return SelectionKey.OP_ACCEPT;
		}
	}

	static class TestReactorEventHandler extends DisruptedAsyncChannelEventHandler {
		
		TestReactorEventHandler(
				DisruptedReactor reactor,
				SelectableChannel channel,
				RingBuffer<SelectionEvent> ringBuffer) {
			super(reactor, channel, SelectionKey.OP_READ, ringBuffer);
		}
	
		@Override
		public int processWrite(SelectionKey key) throws IOException {
			System.out.println("processWrite");
			buffer.clear();
			buffer.put("ACK\n".getBytes());
			buffer.flip();
			((WritableByteChannel)channel).write(buffer);
			
			return SelectionKey.OP_READ;
		}
	
		ByteBuffer buffer = ByteBuffer.allocate(1024);
		@Override
		public int processRead(SelectionKey key) throws IOException {
			System.out.println("processRead");
			
			buffer.clear();
			
			int bytesRead = ((ReadableByteChannel)channel).read(buffer);
			if (bytesRead < 0)
			{
				//throw new ClosedChannelException();
				return AsyncChannelEventHandler.CHANNEL_CLOSED;
			}
			else
			{
				String s = new String(buffer.array(), 0, buffer.position());
				System.out.println(s);

				return SelectionKey.OP_READ | SelectionKey.OP_WRITE;
			}
			
		}
	}

}

