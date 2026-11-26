package io.spring.streamsource.task;

import io.spring.streamsource.Exchange;
import io.spring.streamsource.Sink;
import io.spring.streamsource.SinkCompleteException;
import io.spring.streamsource.Source;

/**
 * Contract for a task that pushes items into a {@link Sink}.
 *
 * <p>This is the push-style alternative to implementing {@link Source}. Rather
 * than producing an item each time a consumer calls {@code receiveNext()}, a
 * Producer runs a loop that pushes items into a {@link Sink}, typically an
 * {@link Exchange} allowing an independent producer and consumer.
 *
 * <pre class="code">
 * Producer&lt;String&gt; producer = sink -&gt; {
 *     sink.send("one");
 *     sink.send("two");
 * };
 * </pre>
 *
 * <p>You can adapt any {@code Source} to {@code Producer} via {@link #fromSource}.
 * The loop to bridge from Source to Sink is trivial.
 *
 * <p>Typically, a {@code Producer} runs in a background thread, pushing into an
 * Exchange, while a consumer receives on the current thread. This pattern is
 * supported by {@link SourceTask}.
 *
 * <p><strong>Note:</strong> Producers should not assume that {@code Sink}
 * supports concurrent, see the Javadoc of {@link Sink}.
 *
 * @param <T> the type of items produced
 * @see Source
 * @see SourceTask
 */
@FunctionalInterface
public interface Producer<T> {

	/**
	 * Produce items into the given {@code Sink}, blocking the caller until there
	 * are no more items to send, or the {@code Sink} can no longer accept.
	 * <p>Implementations don't need to complete the {@code Sink}. It is up to
	 * the caller to decide when to do so. Implementations may handle exceptions
	 * or let them propagate. Generally, a caller is expected to use
	 * {@link #produceAndComplete(Sink)} if necessary to guarantee Sink completion.
	 * <p><strong>Note:</strong> Producers should not assume that {@code Sink}
	 * supports concurrent, see the Javadoc of {@link Sink}.
	 * @param sink the Sink to push items into
	 * @throws Throwable an error encountered by the producer, which the caller
	 * may use to complete the {@code Sink} exceptionally
	 */
	void produce(Sink<T> sink) throws Throwable;

	/**
	 * Delegate to {@link #produce} and complete the {@code Sink} in any outcome.
	 * <p>The default implementation does the following:
	 * <ul>
	 * <li>Completes the {@code Sink} normally if the Producer returned normally.
	 * <li>Completes the {@code Sink} exceptionally if the Producer fails.
	 * <li>Completes the {@code Sink} exceptionally for {@code InterruptedException},
	 * wrapping it as {@link RuntimeException}, and restoring the interrupted state
	 * of the Thread.
	 * </ul>
	 * <p>This method does not throw. Completion is idempotent, so a
	 * {@code Sink} that is already completed, or closed by the consumer, is not
	 * affected.
	 * @param sink the Sink to push items into
	 */
	default void produceAndComplete(Sink<T> sink) {
		try {
			produce(sink);
			sink.complete();
		}
		catch (InterruptedException ex) {
			Thread.currentThread().interrupt();
			sink.completeExceptionally(new RuntimeException("Producer interrupted", ex));
		}
		catch (Throwable ex) {
			sink.completeExceptionally(ex);
		}
	}


	/**
	 * Create a {@code Producer} that receives from the given {@code Source} in
	 * a loop, and sends each item to the {@code Sink}. The {@code Source} is
	 * consumed from in a try-with-resources block, and guaranteed to be closed
	 * for any outcome.
	 * <p>The returned Producer can be used only once since the {@code Source}
	 * is closed, and a second loop will not receive any more items.
	 * @param <T> the type of items received and produced
	 * @param source the source to receive from
	 * @return the producer instance
	 */
	static <T> Producer<T> fromSource(Source<T> source) {
		return new SourceProducerAdapter<>(source);
	}

}
