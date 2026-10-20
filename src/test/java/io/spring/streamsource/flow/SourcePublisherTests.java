package io.spring.streamsource.flow;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Flow.Publisher;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import io.spring.streamsource.AbstractSource;
import io.spring.streamsource.Source;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.Test;
import reactor.adapter.JdkFlowAdapter;
import reactor.test.StepVerifier;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for {@link SourcePublisher}, created through
 * {@link FlowSourceAdapter#toPublisher(Source)}.
 */
public class SourcePublisherTests {

	@Test
	public void emitsItemsAndCompletes() {
		ListSource<String> source = new ListSource<>("a", "b", "c");
		StepVerifier.create(JdkFlowAdapter.flowPublisherToFlux(FlowSourceAdapter.toPublisher(source)))
				.expectNext("a", "b", "c")
				.verifyComplete();
	}

	@Test
	public void ioExceptionReachesSubscriberUnchanged() {
		IOException exception = new IOException("boom");
		Source<String> source = new AbstractSource<>() {
			@Override
			protected String receiveItem() throws IOException {
				throw exception;
			}
		};
		StepVerifier.create(JdkFlowAdapter.flowPublisherToFlux(FlowSourceAdapter.toPublisher(source)))
				.verifyErrorSatisfies(ex -> assertThat(ex).isSameAs(exception));
	}

	@Test
	public void receivesOnlyWhatIsRequested() {
		ListSource<String> source = new ListSource<>("a", "b", "c");
		StepVerifier.create(JdkFlowAdapter.flowPublisherToFlux(FlowSourceAdapter.toPublisher(source)), 2)
				.expectNext("a", "b")
				.then(() -> assertThat(source.receives).hasValue(2))
				.thenRequest(1)
				.expectNext("c")
				.expectNoEvent(Duration.ofMillis(100))
				.then(() -> assertThat(source.receives).hasValue(3))
				.thenRequest(1)
				.verifyComplete();
		assertThat(source.receives).hasValue(4);
	}

	@Test
	public void cancelClosesSourceAndUnblocksReceive() throws InterruptedException {
		BlockingSource source = new BlockingSource();
		StepVerifier.create(JdkFlowAdapter.flowPublisherToFlux(FlowSourceAdapter.toPublisher(source)))
				.then(() -> await(source.receiving))
				.thenCancel()
				.verify();
		// cancel() interrupts the loop thread rather than closing synchronously; this is
		// proof the interrupt reached the blocked receive, not just that it eventually
		// returned some other way
		assertThat(source.interrupted.await(5, TimeUnit.SECONDS)).isTrue();
	}

	@Test
	public void secondSubscriberIsRejected() {
		Publisher<String> publisher = FlowSourceAdapter.toPublisher(new ListSource<>("a"));
		StepVerifier.create(JdkFlowAdapter.flowPublisherToFlux(publisher)).expectNext("a").verifyComplete();
		StepVerifier.create(JdkFlowAdapter.flowPublisherToFlux(publisher)).verifyError(IllegalStateException.class);
	}

	@Test
	public void runsOnVirtualThreadByDefault() {
		ThreadRecordingSource source = new ThreadRecordingSource();
		StepVerifier.create(JdkFlowAdapter.flowPublisherToFlux(FlowSourceAdapter.toPublisher(source))).verifyComplete();
		assertThat(source.virtual).isTrue();
	}

	@Test
	public void runsOnGivenExecutor() {
		ThreadRecordingSource source = new ThreadRecordingSource();
		source.virtual.set(true);
		ExecutorService executor = Executors.newSingleThreadExecutor();
		try {
			StepVerifier.create(JdkFlowAdapter.flowPublisherToFlux(FlowSourceAdapter.toPublisher(source, executor))).verifyComplete();
		}
		finally {
			executor.shutdownNow();
		}
		assertThat(source.virtual).isFalse();
	}

	@Test
	public void requestZeroSignalsIllegalArgumentException() throws InterruptedException {
		ListSource<String> source = new ListSource<>("a", "b", "c");
		StepVerifier.create(JdkFlowAdapter.flowPublisherToFlux(FlowSourceAdapter.toPublisher(source)), 0)
				.consumeSubscriptionWith(subscription -> subscription.request(0))
				.verifyError(IllegalArgumentException.class);
		// Cancelled from onSubscribe, before the loop was ever scheduled: closing the
		// Source is handed off to the executor rather than skipped
		assertThat(source.closed.await(5, TimeUnit.SECONDS)).isTrue();
	}

	@Test
	public void requestZeroWhileRunningSignalsIllegalArgumentException() {
		BlockingSource source = new BlockingSource();
		StepVerifier.create(JdkFlowAdapter.flowPublisherToFlux(FlowSourceAdapter.toPublisher(source)))
				.then(() -> await(source.receiving))
				.consumeSubscriptionWith(subscription -> subscription.request(0))
				.verifyError(IllegalArgumentException.class);
	}

	@Test
	public void roundTrip() throws IOException, InterruptedException {
		ListSource<String> source = new ListSource<>("a", "b", "c");
		try (Source<String> result = FlowSourceAdapter.toSource(FlowSourceAdapter.toPublisher(source))) {
			List<String> items = new ArrayList<>();
			while (result.receiveNext()) {
				items.add(result.next());
			}
			assertThat(items).containsExactly("a", "b", "c");
		}
	}


	private static void await(CountDownLatch latch) {
		try {
			assertThat(latch.await(5, TimeUnit.SECONDS)).isTrue();
		}
		catch (InterruptedException ex) {
			throw new IllegalStateException(ex);
		}
	}

	private static final class ListSource<T> extends AbstractSource<T> {

		private final Iterator<T> iterator;

		private final AtomicInteger receives = new AtomicInteger();

		private final CountDownLatch closed = new CountDownLatch(1);

		@SafeVarargs
		ListSource(T... items) {
			this.iterator = Arrays.asList(items).iterator();
		}

		@Override
		protected @Nullable T receiveItem() {
			this.receives.incrementAndGet();
			return (this.iterator.hasNext() ? this.iterator.next() : null);
		}

		@Override
		protected void handleClose(@Nullable Throwable cause) {
			this.closed.countDown();
		}
	}


	private static final class ThreadRecordingSource extends AbstractSource<String> {

		private final AtomicBoolean virtual = new AtomicBoolean();

		@Override
		protected @Nullable String receiveItem() {
			this.virtual.set(Thread.currentThread().isVirtual());
			return null;
		}
	}


	/**
	 * Source whose receive blocks, interruptibly, until cancelled.
	 */
	private static final class BlockingSource extends AbstractSource<String> {

		private final CountDownLatch receiving = new CountDownLatch(1);

		private final CountDownLatch neverCompletes = new CountDownLatch(1);

		private final CountDownLatch interrupted = new CountDownLatch(1);

		@Override
		protected @Nullable String receiveItem() throws InterruptedException {
			this.receiving.countDown();
			try {
				this.neverCompletes.await();
			}
			catch (InterruptedException ex) {
				this.interrupted.countDown();
				throw ex;
			}
			return null;
		}
	}

}
