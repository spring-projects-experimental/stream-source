package io.spring.streamsource;

/**
 * A {@link Source} and {@link Sink} connected into a pipe that decouples a
 * producer from a consumer. The producer sends items into the {@code Sink} side,
 * and the consumer receives from the {@code Source} side in the same order.
 *
 * <pre class="code">
 * Exchange&lt;String&gt; exchange = new BlockingQueueExchange&lt;&gt;();
 *
 * // producer side, typically on separate thread
 * exchange.send("one");
 * exchange.send("two");
 * exchange.complete();
 *
 * // consumer side
 * try (exchange) {
 *     while (exchange.receiveNext()) {
 *         String item = exchange.next();
 *     }
 * }
 * </pre>
 *
 * <p>Producer and consumer run on separate threads, independent of each other,
 * and with backpressure. Implementations block in {@link #receiveNext()} when
 * there are no items to receive, and likewise in {@link #send} when the
 * Exchange cannot accept more items.
 *
 * <p>Sink and Source are asymmetric in regard to lifecycle:
 * <ul>
 * <li>The producer can complete the {@code Sink} to signal the end of the
 * stream, but the exchange remains open untill all items are received orderly.
 * Only then the Exchange becomes closed.
 * <li>The consumer can close the Exchange at any time, in effect discarding any
 * in-flight items. Subsequent calls to {@code send} on the Sink side throw
 * {@link SinkCompleteException}.
 * </ul>
 *
 * <p>If interrupted in {@code send}, an Exchange must complete exceptionally,
 * allowing the consumer to receive sent items and then see the error.
 * If interrupted in {@code receiveNext()}, an Exchange must close, so the
 * producer will get a {@code SinkCompleteException} on the next send. See
 * <a href="https://github.com/spring-projects-experimental/stream-source/blob/main/CANCELLATION.md">CANCELLATION.md</a>.
 *
 * <p>To run a producer in the background and receive from the Exchange,
 * see {@link io.spring.streamsource.task.ActiveSource}.
 *
 * @see BlockingQueueExchange
 * @see io.spring.streamsource.task.ActiveSource
 *
 * @param <T> the type of items passed through the Exchange
 */
public interface Exchange<T> extends Source<T>, Sink<T> {

}
