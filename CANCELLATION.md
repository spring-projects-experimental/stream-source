
There are cases where *stream-source* needs to stop a background task it started:

- [SourceTask](src/main/java/io/spring/streamsource/task/SourceTask.java) runs a producer task that must be stopped if the consumer goes away.
- [FlowSourceAdapter](src/main/java/io/spring/streamsource/flow/FlowSourceAdapter.java) runs a task to receive from a `Source`,
  and it must be stopped if the downstream `Subscriber` cancels.

In Java, [Thread interrupt](https://docs.oracle.com/en/java/javase/25/docs/api/java.base/java/lang/Thread.html#interrupt()) is the mechanism to stop an executing thread.
It is what `Future.cancel(true)` and `ExecutorService.shutdownNow()` do, and it is what [Structured Concurrency](https://openjdk.org/jeps/543#Cancellation) uses as well.

Thread interrupt is not equally effective everywhere.
Whether it unblocks a thread, and what happens to the underlying resource, depends on the JDK API, the type of resource, and whether the thread is platform or virtual.

Here we provide a brief introduction to the topic:

- [How thread interrupt works](#how-thread-interrupt-works)
- [Support for interruption in the JDK library](#interrupt-support-in-jdk-libraries-)
- [What this means for stream-source](#what-this-means-for-stream-source)
- [Alternatives](#alternatives)


# How Thread Interrupt Works?

An interrupt is cooperative.
It sets the interrupt status of the thread, and leaves it to the code running on that thread to respond.
Blocking methods in the JDK that support interruption wake up and throw an exception, typically `InterruptedException`.
Other code notices the interrupt only if it checks the Thread interrupt status.

There are three possible outcomes from interrupting a blocked method call:

- **Unblocked, resource intact** — the call throws, but whatever it was waiting on remains usable.
  This is typical for `java.util.concurrent` utilities.
- **Unblocked, resource closed** — the call throws, and the underlying resource is closed.
  This is typical where interrupts are supported.
- **Ignored** — the interrupt status is set, but the thread remains blocked until the call returns on its own.

Code that catches `InterruptedException` should not swallow it.
If it cannot rethrow, it should restore the interrupt status with `Thread.currentThread().interrupt()`, so that code further up can notice it.


# Interrupt Support in JDK Libraries 

## Network I/O

Classic `java.net` sockets — `Socket`, `ServerSocket`, and `DatagramSocket` — behave differently by type of thread:

- On a platform thread, an interrupt is ignored.
  A blocked read or accept remains blocked.
- On a virtual thread (Java 21+), an interrupt unblocks the call with a `SocketException`, and closes the socket.
  [JEP 444](https://openjdk.org/jeps/444#Networking) made these operations interruptible "to allow for interruption and cancellation".

NIO channels such as `SocketChannel` and `DatagramChannel` are interruptible by design and implement `InterruptibleChannel`.
An interrupt unblocks the call with `ClosedByInterruptException` on any type of thread, and always closes the channel.

NIO selectors, `Selector.select()` returns early when interrupted, as if `wakeup()` had been called.
It does not throw, and does not close anything, leaving the interrupt status set for the caller to check.


## Files

`FileInputStream` and `FileOutputStream` ignore interrupts on any type of thread.
On a virtual thread, the JDK compensates for the blocked carrier thread, but the call itself still cannot be interrupted.
In practice, reads and writes on local files are usually short.

`FileChannel` is an `InterruptibleChannel`.
An interrupt closes the channel and the call throws `ClosedByInterruptException`, but only after an in-progress read or write returns.
A practical consequence is that the channel is closed on the thread that calls `interrupt()`, which then waits for the I/O in progress to finish.


## Standard I/O and Child Processes

`System.in`, `System.out`, and the streams of a child `Process` are backed by file descriptors, like file streams.
They ignore interrupts on any type of thread, and do not close.
Unlike local files, these can block indefinitely, waiting for input on `System.in`, writing to `System.out`, or reading from a child process that has not produced any output.

Unblocking such a call is possible only by closing the stream, or ending the child process from another thread.


## DNS

Host name lookups through `InetAddress` use the native resolver of the OS, and ignore interrupts on any type of thread.
A lookup completes or fails according to the timeouts configured for the OS resolver.

Since Java 18, [JEP 418](https://openjdk.org/jeps/418) allows plugging in an alternative resolver, which can be implemented to respond to interrupts.
There are 3rd party libraries that build on this.


## JDBC

Most drivers communicate over classic socket I/O, and behave accordingly:

- On a platform thread, an interrupt is typically ignored until the database responds.
- On a virtual thread, an interrupt typically unblocks the call with an `SQLException`, and closes the connection.
  A closed connection cannot be returned to a pool for reuse.

JDBC has its own mechanism for cancellation, `Statement.cancel()`, which `Statement.setQueryTimeout()` typically relies on.
Some drivers, but not all, implement this by sending a separate, out-of-band request to the database server.
The server then stops the query in an orderly way, and the connection remains usable, including when it is pooled.
Where a driver does not support this, `Statement.cancel()` may not stop the query promptly, or at all.


## JDK Concurrency Mechanisms

Most ways to wait in the JDK respond to interrupts by throwing `InterruptedException`, and nothing is closed:

- `Thread.sleep()`, `Thread.join()`, and `Object.wait()`
- `BlockingQueue.put()` and `take()`, and the timed `offer()` and `poll()`
- `Lock.lockInterruptibly()` and `Condition.await()`
- `CountDownLatch.await()`, `Semaphore.acquire()`, and `CyclicBarrier.await()`
- `Future.get()`, including `CompletableFuture.get()`

`LockSupport.park()` returns when the thread is interrupted, but does not throw.
Code that parks needs to check the interrupt status itself.


### Not Interruptible

Some wait mechanisms ignore interrupts completely:

- `synchronized`
- `ReentrantLock.lock()`, as opposed to `lockInterruptibly()`
- `Condition.awaitUninterruptibly()` and `Semaphore.acquireUninterruptibly()`
- `CompletableFuture.join()`, as opposed to `get()`


## Summary

| API / Resource | Platform thread | Virtual thread | Resource closed? | Other ways to unblock |
|---|---|---|---|---|
| `Socket`, `ServerSocket`, `DatagramSocket` | ignored | unblocks | yes, on a virtual thread | close the socket, `SO_TIMEOUT` |
| NIO channels, e.g. `SocketChannel` | unblocks | unblocks | yes | close the channel |
| `Selector.select()` | returns | returns | no | `wakeup()` |
| `FileInputStream`, `FileOutputStream` | ignored | ignored | no | usually short for local files |
| `FileChannel` | unblocks, once the I/O returns | unblocks, once the I/O returns | yes | close the channel |
| `System.in`, `System.out`, `Process` streams | ignored | ignored | no | close the stream, `Process.destroy()` |
| `Process.waitFor()` | unblocks | unblocks | no, the process keeps running | `Process.destroy()` |
| DNS through `InetAddress` | ignored | ignored | no | OS resolver timeouts, JEP 418 resolver |
| JDBC (most drivers) | ignored | unblocks | yes, the connection | `Statement.cancel()`, if supported by the driver |
| `java.util.concurrent` waits, `sleep`, `join`, `wait` | unblocks | unblocks | no | |
| `synchronized`, `ReentrantLock.lock()`, `CompletableFuture.join()` | ignored | ignored | no | |


# What This Means for stream-source

When `SourceTask` is closed, the background producer task is interrupted.
When `Flow.Publisher` is cancelled, the background receiving task is interrupted.
The interrupt may or may not take effect immediately, as described in the previous section [Support for Interruption](#interrupt-support-in-jdk-libraries-).

It works significantly better on Java 21+, where tasks run on virtual threads by default, and therefore blocking operations on classic socket I/O respond to interrupts.

If the task does not respond to the interrupt, it remains blocked until the blocked method times out or returns on its own.
After that, the interrupt is noticed and the task stops.


# Alternatives

This section discussed alternatives for when a  `Source` does not respond to interrupts.


## Closing from a Separate Thread

Some resources that ignore interrupts can be unblocked by closing them from another thread.
For example, closing a socket unblocks a thread reading from it.
Closing the stream of a child process or calling `Process.destroy()` unblocks a thread reading its output.
`Statement.cancel()` stops a query, if the driver supports it.

There is currently no built-in support to that in *stream-source*.
However, it is possible to do it through a `Source` decorator.


## Timed Polling

If a resource supports receiving with a timeout, a `Source` can receive in a loop with a short timeout, and check the interrupt status in between.

For example, a `Socket` with `SO_TIMEOUT` on a platform thread:

```java
class SocketSource extends AbstractSource<ByteBuffer> {

    private final Socket socket;

    private final InputStream input;

    SocketSource(Socket socket) throws IOException {
        this.socket = socket;
        this.socket.setSoTimeout(100);
        this.input = socket.getInputStream();
    }

    @Override
    protected @Nullable ByteBuffer receiveItem() throws IOException, InterruptedException {
        byte[] buffer = new byte[8192];
        while (true) {
            try {
                int read = this.input.read(buffer);
                return (read != -1 ? ByteBuffer.wrap(buffer, 0, read) : null);
            }
            catch (SocketTimeoutException ex) {
                if (Thread.interrupted()) {
                    throw new InterruptedException();
                }
            }
        }
    }

    @Override
    protected void handleClose(@Nullable Throwable failure) throws IOException {
        this.socket.close();
    }
}
```

A read that times out throws `SocketTimeoutException`, and leaves the socket usable, so the `Source` checks the interrupt status and reads again.
An interrupt is then noticed within one poll interval.

