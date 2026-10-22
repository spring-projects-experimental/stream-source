package io.spring.streamsource.flow;

import java.util.concurrent.Executor;
import java.util.concurrent.Flow.Publisher;

import io.spring.streamsource.BlockingQueueExchange;
import io.spring.streamsource.Source;
import org.jspecify.annotations.Nullable;

/**
 * Adapters between {@link Source} and JDK {@link java.util.concurrent.Flow.Publisher},
 * the Reactive Streams interfaces shipped with Java.
 *
 * <p>A {@code Source} and a {@code Publisher} describe the same stream from opposite
 * ends: one is pulled by a blocking consumer, the other pushes to a subscriber that
 * signals demand. Both directions of the adapter preserve backpressure, so neither side
 * receives items nobody asked for:
 * <ul>
 * <li>{@link #toPublisher(Source)} receives on a background thread, and only while there
 * is outstanding demand. Cancelling closes the Source.
 * <li>{@link #toSource(Publisher)} subscribes on the first receive, and requests as many
 * items as the buffer can hold, one more each time the consumer takes one out. Closing
 * the Source cancels the subscription.
 * </ul>
 */
public abstract class FlowSourceAdapter {

	private static final @Nullable Executor defaultExecutor = createDefaultExecutor();

	private FlowSourceAdapter() {
	}


	/**
	 * Adapt a {@link Source} to a {@link Publisher} that receives from it on a virtual
	 * thread.
	 * <p>The Source is not touched until the Publisher is subscribed to, and is closed
	 * when the stream ends, fails, or is cancelled. Since a Source has a single consumer,
	 * the returned Publisher accepts a single subscriber, and a second one is rejected
	 * with an {@link IllegalStateException}.
	 * <p>The end of the stream is discovered by a receive like any other, so it is
	 * signalled by the {@code request(n)} that follows the last item: a subscriber that
	 * stops requesting after exactly as many items as the Source had does not see
	 * {@code onComplete} until it asks again.
	 * <p>Cancelling never blocks the calling thread: a receive in progress is interrupted,
	 * and the receive loop then closes the Source.
	 * <p>Requires Java 21+, use {@link #toPublisher(Source, Executor)} on Java &lt; 21.
	 * @param source the source to receive from
	 * @return a Publisher that emits the items of the Source
	 * @param <T> the type of items
	 * @throws IllegalStateException if virtual threads are not supported
	 */
	public static <T> Publisher<T> toPublisher(Source<T> source) {
		Executor executor = defaultExecutor;
		if (executor == null) {
			throw new IllegalStateException("Virtual threads require Java 21+, use " +
					"toPublisher(Source, Executor) instead");
		}
		return toPublisher(source, executor);
	}

	/**
	 * Variant of {@link #toPublisher(Source)} with the {@code Executor} to receive on.
	 * The receive loop blocks, and parks while there is no demand, so this must be an
	 * executor where blocking is allowed, compatible with Java &lt; 21. The caller
	 * remains responsible for its lifecycle.
	 * @param source the source to receive from
	 * @param executor the executor that runs the receive loop
	 * @return a Publisher that emits the items of the Source
	 * @param <T> the type of items
	 */
	public static <T> Publisher<T> toPublisher(Source<T> source, Executor executor) {
		return new SourcePublisher<>(source, executor);
	}

	/**
	 * Adapt a {@link Publisher} to a {@link Source} that buffers up to
	 * {@link BlockingQueueExchange#DEFAULT_CAPACITY} items ahead of the consumer.
	 * @param publisher the publisher to subscribe to
	 * @return a Source over the items of the publisher
	 * @param <T> the type of items
	 */
	public static <T> Source<T> toSource(Publisher<T> publisher) {
		return toSource(publisher, BlockingQueueExchange.DEFAULT_CAPACITY);
	}

	/**
	 * Variant of {@link #toSource(Publisher)} with the number of items to buffer ahead
	 * of the consumer, which is also the demand the publisher is given.
	 * <p>The subscription happens on the first receive, and is cancelled when the
	 * Source is closed. The publisher keeps pushing on its own thread, into a buffer
	 * the consumer pulls from.
	 * @param publisher the publisher to subscribe to
	 * @param capacity the number of items to buffer, and to request
	 * @return a Source over the items of the publisher
	 * @param <T> the type of items
	 */
	public static <T> Source<T> toSource(Publisher<T> publisher, int capacity) {
		return new PublisherSource<>(publisher, capacity);
	}

	private static @Nullable Executor createDefaultExecutor() {
		try {
			return new FlowVirtualThreadDelegate().newVirtualThreadPerTaskExecutor();
		}
		catch (UnsupportedOperationException ex) {
			return null;
		}
	}

}
