package io.spring.streamsource.flow;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Flow.Publisher;
import java.util.concurrent.Flow.Subscriber;
import java.util.concurrent.Flow.Subscription;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import io.spring.streamsource.Source;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import reactor.blockhound.BlockHound;
import reactor.core.scheduler.Schedulers;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Checks under BlockHound that {@link PublisherSource}'s {@code onNext} never waits for
 * room in the buffer. Items are pushed as fast as demand allows from
 * {@link Schedulers#parallel()}, which BlockHound treats as non-blocking, against a slow
 * consumer and a small capacity, so the buffer is full most of the time.
 *
 * <p>{@code onNext} is not lock-free: it takes the buffer queue's locks, and a brief
 * contention for them with the consumer is not something this test reliably provokes.
 */
public class PublisherSourceBlockHoundTests {

	static {
		BlockHound.install();
	}

	@Test
	@Timeout(10)
	void onNextUnderPressureNeverBlocks() throws InterruptedException, java.io.IOException {
		PumpingPublisher publisher = new PumpingPublisher();
		Source<String> source = FlowSourceAdapter.toSource(publisher, 4);

		AtomicReference<Throwable> caught = new AtomicReference<>();
		CountDownLatch stop = new CountDownLatch(1);
		Schedulers.parallel().schedule(() -> {
			try {
				publisher.pumpUntil(stop);
			}
			catch (Throwable ex) {
				caught.set(ex);
			}
		});

		// Deliberately slow, intermittent consumer: the pump above will have the
		// buffer completely full for most of this run.
		int received = 0;
		long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(3);
		while (System.nanoTime() < deadline) {
			if (source.receiveNext()) {
				source.next();
				received++;
			}
			Thread.sleep(1);
		}
		stop.countDown();
		source.close();

		assertThat(caught.get()).isNull();
		assertThat(received).isGreaterThan(0);
		assertThat(publisher.sent.get()).isGreaterThan(0);
	}


	/**
	 * A minimal, demand-respecting {@link Publisher} whose {@link #pumpUntil} sends
	 * items as fast as granted demand allows, from whatever thread calls it.
	 */
	private static final class PumpingPublisher implements Publisher<String> {

		final AtomicLong demand = new AtomicLong();

		final AtomicLong sent = new AtomicLong();

		final CountDownLatch subscribed = new CountDownLatch(1);

		private final AtomicReference<@Nullable Subscriber<? super String>> subscriber = new AtomicReference<>();

		@Override
		public void subscribe(Subscriber<? super String> subscriber) {
			this.subscriber.set(subscriber);
			subscriber.onSubscribe(new Subscription() {
				@Override
				public void request(long n) {
					demand.getAndUpdate(d -> {
						long sum = d + n;
						return (sum < 0 ? Long.MAX_VALUE : sum);
					});
				}
				@Override
				public void cancel() {
				}
			});
			this.subscribed.countDown();
		}

		void pumpUntil(CountDownLatch stop) {
			// A blocking await() here would itself trip BlockHound, since this runs
			// entirely on the simulated non-blocking thread; spin instead.
			while (this.subscribed.getCount() > 0 && stop.getCount() > 0) {
				// busy-wait for subscribe()
			}
			Subscriber<? super String> actual = this.subscriber.get();
			while (stop.getCount() > 0) {
				if (this.demand.get() > 0) {
					this.demand.decrementAndGet();
					actual.onNext("x");
					this.sent.incrementAndGet();
				}
			}
		}
	}
}
