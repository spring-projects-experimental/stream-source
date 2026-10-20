package io.spring.streamsource.flow;

import java.io.IOException;
import java.util.Objects;
import java.util.concurrent.Flow.Publisher;
import java.util.concurrent.Flow.Subscriber;
import java.util.concurrent.Flow.Subscription;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import io.spring.streamsource.BlockingQueueExchange;
import io.spring.streamsource.SinkCompleteException;
import io.spring.streamsource.Source;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@link Source} that subscribes to a Reactive Streams {@link Publisher} and buffers
 * what it pushes, so that the consumer can pull. Created through
 * {@link FlowSourceAdapter#toSource(Publisher)}.
 *
 * <p>The buffer is a {@link BlockingQueueExchange}, and it doubles as the demand window:
 * {@code capacity} items are requested up front, and one more each time the consumer
 * takes an item out. So the number of buffered items plus the outstanding demand never
 * exceeds the capacity, {@code onNext} always has room, and nothing has to block on the
 * publisher's thread.
 *
 * <p>No thread is needed for this direction. The publisher pushes on whatever thread it
 * already had, and the consumer's own thread is the one that blocks.
 *
 * @param <T> the type of items received through the Source
 */
final class PublisherSource<T> implements Source<T> {

	private static final Logger logger = LoggerFactory.getLogger(PublisherSource.class);


	private final Publisher<T> publisher;

	private final int capacity;

	private final BlockingQueueExchange<T> exchange;

	private final ExchangeSubscriber subscriber = new ExchangeSubscriber();

	private boolean subscribed;

	private boolean closed;


	PublisherSource(Publisher<T> publisher, int capacity) {
		this.publisher = publisher;
		this.capacity = capacity;
		this.exchange = new BlockingQueueExchange<>(capacity);
	}


	@Override
	public boolean receiveNext() throws IOException, InterruptedException {
		if (!this.subscribed) {
			if (this.closed) {
				return false;
			}
			// Subscribe on the first receive, so that creating a Source starts nothing
			logger.debug("subscribing to {}", this.publisher);
			this.subscribed = true;
			this.publisher.subscribe(this.subscriber);
		}
		try {
			if (this.exchange.receiveNext()) {
				// Give the publisher back the slot the item freed
				this.subscriber.request(1);
				return true;
			}
			return false;
		}
		catch (InterruptedException ex) {
			close();
			throw ex;
		}
	}

	@Override
	public T next() {
		return this.exchange.next();
	}

	@Override
	public void close() {
		this.closed = true;
		this.subscriber.cancel(); // stops the inflow, also if not yet subscribed
		this.exchange.close(); // discards buffered items
	}

	@Override
	public String toString() {
		return getClass().getSimpleName() + " for " + this.publisher;
	}


	/**
	 * The subscriber that moves items into the exchange, and records how the publisher
	 * ended in it. The exchange then applies this package's error contract: an
	 * {@code IOException} or a {@code RuntimeException} reaches the consumer unchanged,
	 * anything else wrapped in an {@code IllegalStateException}.
	 */
	private final class ExchangeSubscriber implements Subscriber<T> {

		/** Marks the subscription as cancelled or terminated, including before it arrived. */
		private static final Subscription CANCELLED = new Subscription() {
			@Override
			public void request(long n) {
			}
			@Override
			public void cancel() {
			}
		};

		private final AtomicReference<@Nullable Subscription> subscription = new AtomicReference<>();

		/** Items requested but not yet received. */
		private final AtomicLong outstanding = new AtomicLong();

		@Override
		public void onSubscribe(Subscription subscription) {
			Objects.requireNonNull(subscription, "Subscription must not be null");
			if (this.subscription.compareAndSet(null, subscription)) {
				this.outstanding.addAndGet(capacity);
				subscription.request(capacity);
			}
			else {
				// §2.5 for a second subscription, or the Source was closed already
				subscription.cancel();
			}
		}

		@Override
		public void onNext(T item) {
			Objects.requireNonNull(item, "Item must not be null");
			if (this.subscription.get() == CANCELLED) {
				logger.debug("discarding item received after close: {}", item);
				return;
			}
			if (this.outstanding.decrementAndGet() < 0) {
				// Only reachable if the publisher sent more than it was asked for
				demandExceeded();
				return;
			}
			try {
				if (!exchange.offer(item)) {
					// The demand accounting above should make this unreachable. Should it
					// break, offer fails where send would park this thread on a full
					// queue. Both take the queue's locks, but unlike send, offer does not
					// throw when this thread happens to have its interrupt flag set
					demandExceeded();
				}
			}
			catch (SinkCompleteException ex) {
				// The consumer closed the Source while this item was in flight
				logger.debug("discarding item received after close: {}", item);
				cancel();
			}
		}

		private void demandExceeded() {
			cancel();
			exchange.completeExceptionally(new IllegalStateException(
					"Publisher exceeded its demand of " + capacity + " items: " + publisher));
		}

		@Override
		public void onError(Throwable ex) {
			Objects.requireNonNull(ex, "Throwable must not be null");
			if (terminate()) {
				exchange.completeExceptionally(ex);
			}
			else {
				logger.debug("discarding error received after close: {}", ex.toString());
			}
		}

		@Override
		public void onComplete() {
			if (terminate()) {
				exchange.complete();
			}
		}

		/**
		 * Mark the subscription as done, and report whether it was active.
		 */
		private boolean terminate() {
			return (this.subscription.getAndSet(CANCELLED) != CANCELLED);
		}

		void request(long n) {
			Subscription subscription = this.subscription.get();
			if (subscription != null && subscription != CANCELLED) {
				this.outstanding.addAndGet(n);
				subscription.request(n);
			}
		}

		void cancel() {
			Subscription subscription = this.subscription.getAndSet(CANCELLED);
			if (subscription != null && subscription != CANCELLED) {
				subscription.cancel();
			}
		}
	}

}
