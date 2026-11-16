package io.spring.streamsource.task;

import io.spring.streamsource.Sink;
import io.spring.streamsource.SinkCompleteException;
import io.spring.streamsource.Source;

/**
 * Component that produces items pushed into a {@link Sink}.
 * @param <T> the type of items produced
 */
@FunctionalInterface
public interface Producer<T> {

	/**
	 * Produce items into a {@code Sink}.
	 * <p>Implementations don't need to complete the {@code Sink}, and shouldn't
	 * assume its lifecycle ends after the method returns. Implementations may
	 * handle exceptions or let them propagate. Generally, callers are expected
	 * to use {@link #produceAndComplete(Sink)} to guarantee Sink completion in
	 * any outcome.
	 * @param sink the Sink to push items into
	 * @throws Throwable an unhandled error encountered by the producer
	 */
	void produce(Sink<T> sink) throws Throwable;

	/**
	 * Delegate to {@link #produce} and complete the {@code Sink} if not already.
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
	 * Create a {@code Producer} that receives from a given {@code Source}.
	 * @param source the source to receive from
	 * @return the producer instance
	 * @param <T> the type of items received and produced
	 */
	static <T> Producer<T> fromSource(Source<T> source) {
		return new SourceProducerAdapter<>(source);
	}

}
