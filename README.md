# disrupted-reactor
Java async. IO (selector based) + LMAX Disruptor
------------------------------------------------

Requires Java 1.8.

Idea implemented:
* A dedicated thread for select() - reactor pattern, realized as one disruptor instance with special WaitStrategy.
* N threads (i.e. processors) to process IO events. Processing of one NIO Channel always happens in one thread (this implies code does not need to be thread-safe -> performance increase).
