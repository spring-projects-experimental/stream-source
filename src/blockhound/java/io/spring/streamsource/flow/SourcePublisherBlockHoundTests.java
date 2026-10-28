package io.spring.streamsource.flow;

import java.io.IOException;
import java.nio.channels.spi.AbstractInterruptibleChannel;
import java.util.Arrays;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.Flow.Publisher;
import java.util.concurrent.Flow.Subscriber;
import java.util.concurrent.Flow.Subscription;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.LockSupport;

import io.spring.streamsource.AbstractSource;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.Test;
import reactor.blockhound.BlockHound;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

/**
 * Checks under BlockHound that {@code request(n)} and {@code cancel()} on a
 * {@link SourcePublisher} subscription never block the calling thread. The calls are
 * made from {@link Schedulers#parallel()}, which BlockHound treats as non-blocking,
 * standing in for an event-loop thread.
 */
public class SourcePublisherBlockHoundTests {

	static {
		BlockHound.install();
	}

	@Test
	void cancelDoesNotBlockOnEventLoopEvenWithASlowClose() throws InterruptedException {
		SlowCloseSource source = new SlowCloseSource();
		Subscription subscription = subscribe(
				FlowSourceAdapter.toPublisher(source, Executors.newCachedThreadPool()));
		subscription.request(1);
		assertThat(source.receiving.await(5, TimeUnit.SECONDS)).isTrue();

		// The slow close() has to run on the loop thread, not on the cancelling one
		assertThatCode(() -> Mono.fromRunnable(subscription::cancel)
				.subscribeOn(Schedulers.parallel())
				.block())
				.doesNotThrowAnyException();

		// The loop thread closes the Source asynchronously
		assertThat(source.closed.await(5, TimeUnit.SECONDS)).isTrue();
	}

	@Test
	void cancelDoesNotBlockOnEventLoopWhenTheSourceReadsAChannel() throws InterruptedException {
		ChannelSource source = new ChannelSource();
		Subscription subscription = subscribe(
				FlowSourceAdapter.toPublisher(source, Executors.newCachedThreadPool()));
		subscription.request(1);
		assertThat(source.receiving.await(5, TimeUnit.SECONDS)).isTrue();

		// Interrupting a thread blocked on a channel closes the channel on the interrupting thread
		assertThatCode(() -> Mono.fromRunnable(subscription::cancel)
				.subscribeOn(Schedulers.parallel())
				.block())
				.doesNotThrowAnyException();

		assertThat(source.closed.await(5, TimeUnit.SECONDS)).isTrue();
	}

	@Test
	void concurrentRequestNeverBlocks() throws InterruptedException {
		// Unbounded demand and a fast in-memory Source keep the loop thread emitting,
		// while request(1) is called from a non-blocking thread for the whole run
		String[] items = new String[500_000];
		Arrays.fill(items, "x");
		ListSource<String> source = new ListSource<>(items);
		Publisher<String> publisher = FlowSourceAdapter.toPublisher(source, Executors.newCachedThreadPool());

		AtomicReference<Subscription> subRef = new AtomicReference<>();
		CountDownLatch streamDone = new CountDownLatch(1);
		publisher.subscribe(new Subscriber<String>() {
			@Override public void onSubscribe(Subscription s) {
				subRef.set(s);
				s.request(Long.MAX_VALUE);
			}
			@Override public void onNext(String item) {
			}
			@Override public void onError(Throwable t) {
				streamDone.countDown();
			}
			@Override public void onComplete() {
				streamDone.countDown();
			}
		});
		Subscription subscription = subRef.get();

		AtomicReference<Throwable> caught = new AtomicReference<>();
		Schedulers.parallel().schedule(() -> {
			try {
				while (streamDone.getCount() > 0) {
					subscription.request(1);
				}
			}
			catch (Throwable ex) {
				caught.set(ex);
			}
		});

		assertThat(streamDone.await(20, TimeUnit.SECONDS)).isTrue();
		assertThat(caught.get()).isNull();
	}

	private static <T> Subscription subscribe(Publisher<T> publisher) throws InterruptedException {
		AtomicReference<Subscription> ref = new AtomicReference<>();
		CountDownLatch latch = new CountDownLatch(1);
		publisher.subscribe(new Subscriber<T>() {
			@Override public void onSubscribe(Subscription s) {
				ref.set(s);
				latch.countDown();
			}
			@Override public void onNext(T item) {
			}
			@Override public void onError(Throwable throwable) {
			}
			@Override public void onComplete() {
			}
		});
		assertThat(latch.await(5, TimeUnit.SECONDS)).isTrue();
		return ref.get();
	}


	private static final class SlowCloseSource extends AbstractSource<String> {

		final CountDownLatch receiving = new CountDownLatch(1);

		final CountDownLatch closed = new CountDownLatch(1);

		@Override
		protected @Nullable String receiveItem() throws InterruptedException {
			this.receiving.countDown();
			new CountDownLatch(1).await();
			return null;
		}

		@Override
		protected void handleClose(@Nullable Throwable cause) {
			try {
				Thread.sleep(10);
			}
			catch (InterruptedException ex) {
				Thread.currentThread().interrupt();
			}
			this.closed.countDown();
		}
	}


	/**
	 * Source whose receive blocks in an {@link AbstractInterruptibleChannel} until
	 * interrupted, and whose channel is slow to close.
	 */
	private static final class ChannelSource extends AbstractSource<String> {

		final CountDownLatch receiving = new CountDownLatch(1);

		final CountDownLatch closed = new CountDownLatch(1);

		private final SlowCloseChannel channel = new SlowCloseChannel();

		@Override
		protected @Nullable String receiveItem() throws IOException {
			this.channel.read();
			return null;
		}

		@Override
		protected void handleClose(@Nullable Throwable cause) {
			this.closed.countDown();
		}

		private final class SlowCloseChannel extends AbstractInterruptibleChannel {

			void read() throws IOException {
				begin();
				try {
					receiving.countDown();
					while (isOpen()) {
						LockSupport.park(this);
					}
				}
				finally {
					end(false);
				}
			}

			@Override
			protected void implCloseChannel() {
				try {
					Thread.sleep(10);
				}
				catch (InterruptedException ex) {
					Thread.currentThread().interrupt();
				}
			}
		}
	}


	private static final class ListSource<T> extends AbstractSource<T> {

		private final T[] items;
		private int index;

		ListSource(T[] items) {
			this.items = items;
		}

		@Override
		protected @Nullable T receiveItem() {
			return (this.index < this.items.length ? this.items[this.index++] : null);
		}
	}
}
