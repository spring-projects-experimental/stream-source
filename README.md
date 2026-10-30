## Overview

[Virtual Threads](https://openjdk.org/jeps/444) made blocking cheap.
[Structured Concurrency](https://openjdk.org/jeps/543) will simplify concurrent code.
Modern applications also need a way to stream data (SSE, LLM tokens, MCP responses, etc.).
There is currently no simple, standard way to do that in Java in imperative style.

Historically in Java, we have streamed data through callback interfaces.
Callbacks are good for asynchronous handling.
For imperative code we prefer a simple loop that blocks.
Like `Iterator` but for data from a remote host.
Like `InputStream` but for Objects.
More than API style, we need support for independent producer-consumer, and other essential patterns.

*Stream Source* is a small library that aims to fill this gap.
It is built by the Spring team but does not depend on the Spring Framework or any Spring project.
A core facility for the Java ecosystem with wide applicability.

The Structured Concurrency JEP mentions "channels" as a non-goal that may be proposed in the future.
We aim to fill that gap in the interim with a solution for the use cases we see.
When there is a concrete proposal in the JDK, we will adapt accordingly.

The *Stream Source* library provides contracts and building blocks for data streaming in imperative style.
In addition, there are adapters to bridge to Reactive Streams through the JDK's `Flow` contracts.

For more deatils, see [DESIGN.md](DESIGN.md), [CANCELLATION.md](CANCELLATION.md), and [FAQ.md](FAQ.md).


## Additional Repositories

- [Spring AI Agent](https://github.com/spring-projects-experimental/spring-ai-agent) --
  Agentic abstraction for Spring AI that uses *stream-source*.
- [Stream Source Spring Extensions](https://github.com/spring-projects-experimental/stream-source-spring-extensions) --
  Spring project extensions that use *stream-source*.


## Maven Artefact and Dependencies

To use *Stream Source*, add the following dependency:

```xml
<dependency>
    <groupId>io.spring.streamsource</groupId>
    <artifactId>stream-source</artifactId>
    <version>0.0.1-SNAPSHOT</version>
</dependency>
```

`stream-source` has only two dependencies, [JSpecify](https://jspecify.dev/) and [SLF4J](https://www.slf4j.org/).


## Requirements and Use

`stream-source` has a Java 17+ baseline.
Virtual threads are used by default on Java 21+.

The published artifacts use the `io.spring.streamsource` group ID, and can be installed
locally with `./gradlew publishToMavenLocal`.
