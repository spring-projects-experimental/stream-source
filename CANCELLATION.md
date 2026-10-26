# Cancellation

A blocked `receiveNext()` is cancelled by interrupting the thread that receives.
The Source throws `InterruptedException` and closes.

This is how Java cancels work in general: `Future.cancel(true)`, `ExecutorService.shutdownNow()`, and a structured concurrency scope shutting down its forks all interrupt.
The library does the same.
`ActiveSource` interrupts the producer task when its Source is closed while the task receives.
The `Flow.Publisher` from [FlowSourceAdapter](src/main/java/io/spring/streamsource/flow/FlowSourceAdapter.java)`.toPublisher` interrupts its receiving thread when cancelled during a receive.
A `Source` should therefore expect to be interrupted while it receives.

The library follows the cancellation model of virtual threads and structured concurrency, and intends to keep doing so.
[JEP 444](https://openjdk.org/jeps/444#Networking) made blocking `java.net.Socket` operations interruptible on virtual threads "to allow for interruption and cancellation".
[JEP 543](https://openjdk.org/jeps/543#Cancellation) expects subtasks to "be coded so that they finish as soon as possible when interrupted", and the same applies to a Source while it receives.


## Interruptible Sources

A `Source` can be cancelled by interrupts, if two things hold:

1. **An interrupt unblocks the receive promptly**, on the kind of thread it runs on.
2. **The interrupt only damages what the Source owns.**
   For example, an interrupt closes the socket of a virtual thread blocked reading it, which is fine for a connection the Source owns, but not for one that belongs to a pool.

Most Sources meet both conditions and need nothing special.


## Sources that do not respond to interrupts

A Source that fails the first condition cannot be cancelled while it receives.
Its receive holds the thread until it returns on its own, and so does everything waiting for it:
closing an `ActiveSource` blocks until the producer task returns, and a cancelled `Flow.Publisher` closes the Source only once the receive returns, holding its executor thread until then.
This is the same as for a structured concurrency scope, whose subtasks that do not respond to interrupts "may delay the closing of a scope indefinitely".
A Source that fails the second condition is cancelled, but the interrupt damages a resource beyond the Source, like the pooled connection above.

This is a bigger problem on Java before 21, without virtual threads.
Blocking I/O from `java.io`, like a `Socket` read, ignores interrupts on a platform thread, while on a virtual thread an interrupt closes the socket and unblocks the read.
A platform thread is also a scarce resource, usually from a bounded pool, so every receive that cannot be cancelled takes one away from the rest of the application.

Working around these limitations of interrupts is outside the scope of the library.


## Common Resources

How common resources fare against the two conditions:

| Receiving from | 1. Unblocked by an interrupt | 2. Damage limited to the Source |
|---|---|---|
| `BlockingQueueExchange`, `FlowSourceAdapter.toSource` | yes | yes |
| `SocketChannel`, `FileChannel` | yes, the channel is closed | if the Source owns the channel |
| `Socket` on a virtual thread | yes, the socket is closed | if the Source owns the socket |
| `Socket` on a platform thread | no, unless polled with `SO_TIMEOUT` | |
| JDBC on a virtual thread | yes, the connection is closed | no, if the connection is pooled |
| JDBC on a platform thread | no | |
| `HttpClient.send` | yes, the request is cancelled | yes |
| `HttpClient` body through `BodyHandlers.ofInputStream()` or `ofLines()` | Java 20+ only, earlier versions ignore it | yes |
| `HttpClient` body through `BodyHandlers.ofPublisher()` and `FlowSourceAdapter.toSource` | yes | yes |
| Process pipes, `FileInputStream` | no | |
| Libraries that wait uninterruptibly | no | |


## Timed Polling

A Source that fails only the first condition can meet it itself, if the resource offers a receive with a timeout.
It receives with a timeout and checks the interrupt status in between, for example from a `Socket` with `SO_TIMEOUT` on a platform thread:

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

A read that times out throws `SocketTimeoutException` and leaves the socket valid, so the Source checks the interrupt status and reads again.
An interrupt is then noticed within one poll interval.
It is the interrupt status that tells the Source to stop, not a flag set by `close()`, since a Source is not closed while it receives.
Polling does not help with the second condition: on a virtual thread, an interrupt that arrives during a socket read still closes the socket.
