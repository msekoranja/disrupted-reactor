# disrupted-reactor
Java async. IO (selector based) + LMAX Disruptor
------------------------------------------------

Requires Java 1.8.

One thread for select() - reactor pattern. Realized as one Disruptor instance with special WaitStrategy.
N threads (i.e. processors) to process async. IO events. Processing of one NIO Channels always happens in one thread (this implies code does not need to be thread-safe -> performance increase).
