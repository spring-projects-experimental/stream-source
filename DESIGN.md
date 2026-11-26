# Overview

The *stream-source* library defines a small set of contracts to stream data in imperative code.
A consumer receives data items in a simple loop.
A producer may run independently, on its own thread, with backpressure between the two.

The key building blocks are:

- [Source](#source): used by a consumer to receive in a loop, and implemented to produce the items.
- [Exchange](#exchange): a pipe with [Sink](#sink) side for sending and a `Source` side for receiving.
- [SourceTask](#sourcetask): starts a [Producer](#producer) task in a background thread to push into an `Exchange` that a consumer can receive from.
- [MultiSinkTask](#multisinktask): dispatches items pushed into a `Sink` to any number of `Sink` listeners concurrently, without holding up the producer.
- [Reactive Streams](#reactive-streams-adapters): bridge to Reactive Streams `Publisher` in both directions.

For further insight and context, see [FAQ.md](FAQ.md).


# Source

[Source](src/main/java/io/spring/streamsource/Source.java) is the most central contract.
A consumer uses it to receive items in a loop where items may be received over a network.
It is `AutoCloseable` to release underlying resources such as a connection.

- `receiveNext()` does the actual work to receive the next item. 
   It blocks until a complete item is available, and can throw `IOException` or `InterruptedException`.
   A blocked receive may be cancelled by interrupting the receiving thread, see [CANCELLATION.md](CANCELLATION.md).
- `next()` simply provides access to the received item.
   It is guaranteed to return a value after `receiveNext()` returns `true`.
- The stream ends when `receiveNext()` returns `false` or the consumer calls `close()`.

```java
try (Source<String> source = ...) {
    while (source.receiveNext()) {
        String item = source.next();
        // process item
    }
}
```

## Source Implementations

Implementing `Source` is the easiest way to create a producer that consumers can receive from directly.

```
  Producer                                       Consumer
┌──────────────────┐      ┌──────────────┐      ┌───────────────────────┐
│ Source           │      │              │      │ while (receiveNext()) │
│ implementation   │ ───▶ │    Source    │ ───▶ │     next()            │
│ (e.g. remote     │      │  (interface) │      │                       │
│  connection)     │      │              │      │                       │
└──────────────────┘      └──────────────┘      └───────────────────────┘
```

`AbstractSource` is a convenient base class that implements the receiving loop, exception handling, and lifecycle management.
Subclasses implement `receiveItem()` to return the next item or `null` when there are no more to receive.

A `Source` implementation hierarchy is suitable for adding generic, higher level transformations and capabilities through decoration.


# Exchange

Receiving directly from a `Source` implementation works, but couples producer and consumer in sequential processing.
What if the consumer needs to perform I/O after receiving an item?
The producer would have to wait until the consumer calls `receiveNext()` again.

Typically, producer and consumer can benefit from operating independently of each other.
It allows the producer to run in parallel with the consumer.
Processing rates can diverge, but within limits with backpressure applied if needed.

[Exchange](src/main/java/io/spring/streamsource/Exchange.java) is the mechanism for this.
It is a pipe that a producer can push into and a consumer can receive from.
[Sink](#sink) is the producing side. `Source` is the consuming side.
`Exchange` implements both `Sink` and `Source`.

`BlockingQueueExchange` is an implementation of `Exchange`.
It uses a `BlockingQueue` to hold produced items that a consumer can receive at its own pace.
When the queue is full, `send` blocks. When it's empty, `receiveNext` blocks.
This is the backpressure between producer and consumer.

## Sink

[Sink](src/main/java/io/spring/streamsource/Sink.java) is the sending side of an `Exchange`.
It is a contract to `send` items, followed by completion.
After completion, the `Exchange` remains open until all items are received.
After that it becomes closed and `receiveNext` returns `false`.

```java
Exchange<String> exchange = new BlockingQueueExchange<>();

// producer side, typically on another thread
exchange.send("one");
exchange.send("two");
exchange.complete();

// consumer side
while (exchange.receiveNext()) {
    String item = exchange.next();
}

// exchange is closed
```

A `Sink` may be completed with an exception via `completeExceptionally`.
After completion with an exception, the `Exchange` remains open until all items are received.
After that it becomes closed and `receiveNext` throws the exception.



## Ownership of Data

An `Exchange` completed from the `Sink` side remains open until all items are received.
By contrast, closing from the `Source` side is immediate and remaining items discarded.
The asymmetric nature of the `Sink` and `Source` sides reflects ownership of the data.
The producer can send items and signal completion only.
Once items are placed in the `Exchange`, they belong to the consumer who can receive or discard them.


# SourceTask

`Exchange` provides the means to share a stream of data between two threads.
A common pattern is to start a producer task in a background thread and consume in the current thread.

[SourceTask](src/main/java/io/spring/streamsource/task/SourceTask.java) is the mechanism for this.
It creates an `Exchange`, starts a producer task to push items into it, and returns the `Source` side of the `Exchange` to receive from.

```java
Source<String> producingSource = ... // implementation of Source;
SourceTask<String> sourceTask = SourceTask.fromSource(producingSource);

try (Source<String> source = sourceTask.start()) {
    while (source.receiveNext()) {
        String item = source.next();
        // process item
    }
}
```

The lifecycle of the producer thread is managed by `SourceTask`.
The `close()` method of the returned `Source` cancels the producer task and awaits its completion.
Cancelling interrupts the producer task, and `close()` returns once the task responds, see [CANCELLATION.md](CANCELLATION.md).

By default, the producer task runs on a virtual thread on Java 21+.
For earlier versions, an `ExecutorService` is expected to be passed in.


## Producer

[Producer](src/main/java/io/spring/streamsource/task/Producer.java) is a simple contract for pushing items into a `Sink`.
It is necessary in `SourceTask` to define the producer task to run in the background.

You can turn any `Source` into a producer task through a static, factory method on `Producer`.
The loop to bridge from `Source` to `Sink` is trivial.
`SourceTask` accepts `Source` or `Producer` for the producer task.

While the main purpose of `Producer` is to define a producer task for `SourceTask`, it is also an alternative way to produce items (push style) vs. a `Source` implementation.
This creates a choice and a question which one to use.

For more context, see the answer to the Source vs. Producer question in [FAQ.md](FAQ.md#should-a-producer-implement-source-or-producer).


# MultiSinkTask

`Exchange` connects producer and consumer, but sometimes there may be multiple consumers.
For example, to notify listeners about streamed items, or for pub-sub to deliver streamed items to independent subscribers.

[Sink](#sink) is a suitable contract for a `Listener`. There are two challenges:

1. It's inefficient to invoke blocking listeners in a sequence.
2. The producer is coupled to listener invocation.

[MultiSinkTask](src/main/java/io/spring/streamsource/task/MultiSinkTask.java) is a mechanism for this.
It creates a `Sink` backed by an `Exchange`, starts a background thread to receive from it, and passes every item to all listeners concurrently in fork and join style.

```
  Producer        Sink                    Fan-out task (background)
┌──────────┐      ┌──────────────┐      ┌─────────────────────────────┐      ┌────────────┐
│ send()   │ ───▶ │   Exchange   │ ───▶ │ receiveNext(), then invoke  │ ───▶ │ Sink       │
│          │      │              │      │ all listeners concurrently  │ ───▶ │ listeners  │
└──────────┘      └──────────────┘      │ (fork and join)             │ ───▶ │            │
                                        └─────────────────────────────┘      └────────────┘
```

By pushing into an `Exchange` that's consumed from independently, the `Producer` is decoupled from listener invocation.
Backpressure is applied when the `Exchange` is full, and the capacity is configurable.

The fan-out task ends when the returned `Sink` completes and all items have been processed by all listeners.
Use `awaitCompletion()` to wait for the background fan-out task to end.
Use `cancel()` to stop it early, which interrupts the fan-out task and any listener invocations in progress, see [CANCELLATION.md](CANCELLATION.md).

```java
Sink<String> listener1 = ... // e.g. an Exchange for another consumer
Sink<String> listener2 = ...

MultiSinkTask<String> multiSinkTask = MultiSinkTask.of(List.of(listener1, listener2));
Sink<String> sink = multiSinkTask.start();

sink.send("one");
sink.send("two");
sink.complete();

multiSinkTask.awaitCompletion();
```

A listener that throws an exception is completed exceptionally and removed, without affecting the other listeners.

By default, listeners are invoked on virtual threads on Java 21+.
For earlier versions, an `ExecutorService` is expected to be passed in.

## Dynamic registrations

`MultiSinkTask` accepts dynamic registration of listeners after it has started.
This is useful for pub-sub on a server where listeners come and go.


# Reactive Streams Adapters

There is a wide ecosystem of Reactive Streams Publishers.
This library provides a bridge that can expose those in imperative code as `Source`.
There are adapters in both directions, to and from Reactive Streams.

[FlowSourceAdapter](src/main/java/io/spring/streamsource/flow/FlowSourceAdapter.java) is the main class for the bridge.
It uses `java.util.concurrent.Flow` interfaces from the JDK library, with no extra dependency.
It's easy to connect further to `org.reactivestreams.Publisher` and Reactor `Flux` via built-in adapters included in each.

When a `Source` is exposed as a `Flow.Publisher`, cancelling the subscription interrupts the thread that receives from the `Source`, see [CANCELLATION.md](CANCELLATION.md).

