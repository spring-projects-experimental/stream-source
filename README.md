## Overview

[Virtual Threads](https://openjdk.org/jeps/444) made blocking cheap.
[Structured Concurrency](https://openjdk.org/jeps/543) will simplify concurrent code.
Modern applications also need a way to stream data (SSE, LLM tokens, MCP responses, etc.).
There is currently no simple, standard way to do that in Java in imperative style.

Historically in Java, we have streamed data through callback interfaces.
Callbacks are good for asynchronous handling.
For imperative code we prefer a simple loop that blocks, similar to `Iterator` but for items fetched over a network.
More than API style, we need support for independent producer-consumer, and other essential patterns.

*stream-source* is a small library that aims to fill this gap.
It is built by the Spring team but does not depend on the Spring Framework or any Spring project.
A core facility for the Java ecosystem with wide applicability.

The Structured Concurrency JEP mentions "channels" as a non-goal that may be proposed in the future.
We aim to fill that gap in the interim with a solution for the use cases we see.
When there is a concrete proposal in the JDK, we will adapt to it accordingly.

*stream-source* provides contracts and building blocks for data streaming in imperative style.
In addition, there are adapters to bridge to Reactive Streams through the JDK's `java.util.concurrent.Flow`.

For more details, see [DESIGN.md](DESIGN.md) and [FAQ.md](FAQ.md).


## Additional Repositories

- [stream-source Spring extensions](https://github.com/spring-projects-experimental/stream-source-spring-extensions) --
  Spring project extensions that use *stream-source*.
- Spring AI Agent --
  Agentic abstraction for Spring AI that uses *stream-source* for its streaming API (to be made public shortly).


## Requirements and Use

`stream-source` has a Java 17+ baseline.
Virtual threads are used by default on Java 21+.

Maven artifact coordinates:

```xml
<dependency>
    <groupId>io.spring.streamsource</groupId>
    <artifactId>stream-source</artifactId>
    <version>0.8.0-SNAPSHOT</version>
</dependency>
```

[JSpecify](https://jspecify.dev/) and [SLF4J](https://www.slf4j.org/) are the only required dependencies.

Snapshots via https://repo.spring.io/snapshot.