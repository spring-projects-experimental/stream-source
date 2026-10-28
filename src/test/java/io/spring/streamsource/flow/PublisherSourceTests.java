package io.spring.streamsource.flow;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import io.spring.streamsource.Source;
import org.junit.jupiter.api.Test;
import reactor.adapter.JdkFlowAdapter;
import reactor.core.publisher.Flux;
import reactor.test.publisher.TestPublisher;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Unit tests for {@link PublisherSource}, created through
 * {@link FlowSourceAdapter#toSource(java.util.concurrent.Flow.Publisher)}.
 */
public class PublisherSourceTests {

	@Test
	public void receivesItemsAndCloses() throws IOException, InterruptedException {
		Source<String> source = FlowSourceAdapter.toSource(JdkFlowAdapter.publisherToFlowPublisher(Flux.just("a", "b", "c")));
		assertThat(receiveAll(source)).containsExactly("a", "b", "c");
	}

	@Test
	public void ioExceptionPassesThroughUnchanged() {
		IOException exception = new IOException("boom");
		Source<String> source = FlowSourceAdapter.toSource(JdkFlowAdapter.publisherToFlowPublisher(Flux.error(exception)));
		assertThatThrownBy(source::receiveNext).isSameAs(exception);
	}

	@Test
	public void checkedExceptionIsWrapped() {
		Exception exception = new Exception("boom");
		Source<String> source = FlowSourceAdapter.toSource(JdkFlowAdapter.publisherToFlowPublisher(Flux.error(exception)));
		assertThatThrownBy(source::receiveNext)
				.isExactlyInstanceOf(RuntimeException.class)
				.hasCause(exception);
	}

	@Test
	public void demandIsBoundedByCapacity() throws IOException, InterruptedException {
		AtomicInteger emitted = new AtomicInteger();
		Flux<Integer> flux = Flux.range(1, 100).doOnNext(i -> emitted.incrementAndGet());
		Source<Integer> source = FlowSourceAdapter.toSource(JdkFlowAdapter.publisherToFlowPublisher(flux), 10);
		assertThat(source.receiveNext()).isTrue();
		assertThat(source.next()).isEqualTo(1);
		// 10 requested up front, and 1 more for the item received
		assertThat(emitted).hasValue(11);
		assertThat(receiveAll(source)).hasSize(99);
	}

	@Test
	public void subscribesOnFirstReceiveOnly() throws IOException, InterruptedException {
		AtomicBoolean subscribed = new AtomicBoolean();
		Source<String> source = FlowSourceAdapter.toSource(JdkFlowAdapter.publisherToFlowPublisher(
				Flux.just("a").doOnSubscribe(subscription -> subscribed.set(true))));
		assertThat(subscribed).isFalse();
		assertThat(source.receiveNext()).isTrue();
		assertThat(subscribed).isTrue();
		source.close();
	}

	@Test
	public void closeWithoutReceivingNeverSubscribes() throws IOException, InterruptedException {
		AtomicBoolean subscribed = new AtomicBoolean();
		Source<String> source = FlowSourceAdapter.toSource(JdkFlowAdapter.publisherToFlowPublisher(
				Flux.just("a").doOnSubscribe(subscription -> subscribed.set(true))));
		source.close();
		assertThat(source.receiveNext()).isFalse();
		assertThat(subscribed).isFalse();
	}

	@Test
	public void subscribeFailureClosesSource() throws IOException, InterruptedException {
		IllegalStateException ex = new IllegalStateException("boom");
		Source<String> source = FlowSourceAdapter.toSource(subscriber -> {
			throw ex;
		});
		assertThatThrownBy(source::receiveNext).isSameAs(ex);
		assertThatThrownBy(source::receiveNext).as("should not hang").isSameAs(ex);
	}

	@Test
	public void closeCancelsSubscription() throws Exception {
		TestPublisher<String> publisher = TestPublisher.create();
		Source<String> source = FlowSourceAdapter.toSource(JdkFlowAdapter.publisherToFlowPublisher(publisher));
		CompletableFuture<Boolean> received = receiveAsync(source);
		awaitSubscribed(publisher);
		publisher.next("a");
		assertThat(received.get(5, TimeUnit.SECONDS)).isTrue();
		assertThat(source.next()).isEqualTo("a");
		source.close();
		publisher.assertCancelled();
	}

	@Test
	public void bufferedItemsAreDiscardedOnClose() throws IOException, InterruptedException {
		Source<Integer> source = FlowSourceAdapter.toSource(JdkFlowAdapter.publisherToFlowPublisher(Flux.range(1, 10)), 10);
		assertThat(source.receiveNext()).isTrue();
		assertThat(source.next()).isEqualTo(1);
		source.close();
		assertThat(source.receiveNext()).isFalse();
	}

	@Test
	public void overflowingPublisherFails() throws Exception {
		TestPublisher<String> publisher = TestPublisher.createNoncompliant(TestPublisher.Violation.REQUEST_OVERFLOW);
		Source<String> source = FlowSourceAdapter.toSource(JdkFlowAdapter.publisherToFlowPublisher(publisher), 2);
		CompletableFuture<Boolean> received = receiveAsync(source);
		awaitSubscribed(publisher);
		// At most 3 items requested: 2 up front, and 1 more for the item received
		publisher.next("a", "b", "c", "d");
		assertThat(received.get(5, TimeUnit.SECONDS)).isTrue();
		assertThat(source.next()).isEqualTo("a");
		assertThatThrownBy(() -> receiveAll(source))
				.isInstanceOf(IllegalStateException.class)
				.hasMessageContaining("exceeded its demand");
		publisher.assertCancelled();
	}

	@Test
	public void interruptedReceiveClosesAndCancels() throws Exception {
		TestPublisher<String> publisher = TestPublisher.create();
		Source<String> source = FlowSourceAdapter.toSource(JdkFlowAdapter.publisherToFlowPublisher(publisher));
		AtomicReference<Throwable> failure = new AtomicReference<>();
		Thread thread = Thread.ofVirtual().start(() -> {
			try {
				source.receiveNext();
			}
			catch (Throwable ex) {
				failure.set(ex);
			}
		});
		awaitSubscribed(publisher);
		thread.interrupt();
		thread.join(5000);
		assertThat(failure.get()).isInstanceOf(InterruptedException.class);
		publisher.assertCancelled();
	}


	private static <T> CompletableFuture<Boolean> receiveAsync(Source<T> source) {
		CompletableFuture<Boolean> future = new CompletableFuture<>();
		Thread.ofVirtual().start(() -> {
			try {
				future.complete(source.receiveNext());
			}
			catch (Throwable ex) {
				future.completeExceptionally(ex);
			}
		});
		return future;
	}

	private static void awaitSubscribed(TestPublisher<?> publisher)
			throws InterruptedException, TimeoutException {

		long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
		while (!publisher.wasSubscribed()) {
			if (System.nanoTime() > deadline) {
				throw new TimeoutException("Not subscribed");
			}
			Thread.sleep(1);
		}
	}

	private static <T> List<T> receiveAll(Source<T> source) throws IOException, InterruptedException {
		List<T> items = new ArrayList<>();
		while (source.receiveNext()) {
			items.add(source.next());
		}
		return items;
	}

}
