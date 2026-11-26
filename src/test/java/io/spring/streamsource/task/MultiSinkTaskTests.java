package io.spring.streamsource.task;

import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import io.spring.streamsource.Sink;
import io.spring.streamsource.TestSink;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalStateException;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;

/**
 * Unit tests for {@link MultiSinkTask}.
 */
public class MultiSinkTaskTests {


	@Test
	public void sendAndComplete() throws IOException, InterruptedException {
		TestSink<String> sink1 = TestSink.create();
		TestSink<String> sink2 = TestSink.create();

		MultiSinkTask<String> multiSinkTask = MultiSinkTask.of(List.of(sink1, sink2));

		Sink<String> sink = multiSinkTask.start();
		sink.send("a");
		sink.send("b");
		sink.complete();

		multiSinkTask.awaitCompletion();

		assertThat(sink1.getSentItems()).containsExactly("a", "b");
		assertThat(sink1.getCompletionException()).isNull();

		assertThat(sink2.getSentItems()).containsExactly("a", "b");
		assertThat(sink2.getCompletionException()).isNull();
	}

	@Test
	public void sendAndCompleteExceptionally() throws IOException, InterruptedException {
		IllegalStateException ex = new IllegalStateException("boom");
		TestSink<String> sink1 = TestSink.create();
		TestSink<String> sink2 = TestSink.create();

		MultiSinkTask<String> multiSinkTask = MultiSinkTask.of(List.of(sink1, sink2));

		Sink<String> sink = multiSinkTask.start();
		sink.send("a");
		sink.send("b");
		sink.completeExceptionally(ex);
		sink.complete();

		multiSinkTask.awaitCompletion();

		assertThat(sink1.getSentItems()).containsExactly("a", "b");
		assertThat(sink1.getCompletionException()).isSameAs(ex);

		assertThat(sink2.getSentItems()).containsExactly("a", "b");
		assertThat(sink1.getCompletionException()).isSameAs(ex);
	}

	@Test
	public void removeFailedListener() throws IOException, InterruptedException {
		IllegalStateException ex = new IllegalStateException("boom");

		TestSink<String> sink1 = TestSink.create();
		TestSink<String> sink2 = TestSink.<String>create().exceptionOnItem("b", ex);

		MultiSinkTask<String> multiSinkTask = MultiSinkTask.of(List.of(sink2, sink1));

		Sink<String> sink = multiSinkTask.start();
		sink.send("a");
		sink.send("b");
		sink.send("c");
		sink.complete();

		multiSinkTask.awaitCompletion();

		assertThat(sink1.getSentItems()).containsExactly("a", "b", "c");
		assertThat(sink1.getCompletionException()).isNull();

		assertThat(sink2.getSentItems()).containsExactly("a", "b");
		assertThat(sink2.getCompletionException()).isSameAs(ex);
	}

	@Test
	public void completeWhenAllListenersFail() throws IOException, InterruptedException {
		IllegalStateException ex = new IllegalStateException("boom");

		TestSink<String> sink1 = TestSink.<String>create().exceptionOnItem("a", ex);
		TestSink<String> sink2 = TestSink.<String>create().exceptionOnItem("b", ex);
		TestSink<String> sink3 = TestSink.<String>create().exceptionOnItem("c", ex);

		MultiSinkTask<String> multiSinkTask = MultiSinkTask.of(List.of(sink1, sink2, sink3));

		Sink<String> sink = multiSinkTask.start();
		sink.send("a");
		sink.send("b");
		sink.send("c");
		sink.complete();

		multiSinkTask.awaitCompletion();

		assertThat(sink1.getSentItems()).containsExactly("a");
		assertThat(sink1.getCompletionException()).isSameAs(ex);

		assertThat(sink2.getSentItems()).containsExactly("a", "b");
		assertThat(sink2.getCompletionException()).isSameAs(ex);

		assertThat(sink3.getSentItems()).containsExactly("a", "b", "c");
		assertThat(sink3.getCompletionException()).isSameAs(ex);
	}

	@Test
	public void registerAndUnregisterListenerBeforeStart() throws IOException, InterruptedException {
		TestSink<String> sink1 = TestSink.create();
		TestSink<String> sink2 = TestSink.create();

		MultiSinkTask<String> multiSinkTask = MultiSinkTask.of(List.of(sink1));
		multiSinkTask.registerListener(sink2);
		multiSinkTask.unregisterListener(sink1);

		Sink<String> sink = multiSinkTask.start();
		sink.send("a");
		sink.send("b");
		sink.complete();

		multiSinkTask.awaitCompletion();

		assertThat(sink1.getSentItems()).isEmpty();
		assertThat(sink1.isComplete()).isTrue();

		assertThat(sink2.getSentItems()).containsExactly("a", "b");
		assertThat(sink2.isComplete()).isTrue();
		assertThat(sink2.getCompletionException()).isNull();
	}

	@Test
	public void registerListenerAfterStart() throws IOException, InterruptedException {
		CountDownLatch latch = new CountDownLatch(1);

		TestSink<String> sink1 = TestSink.<String>create().itemHandler("a", _ -> latch.countDown());
		TestSink<String> sink2 = TestSink.create();
		TestSink<String> sink3 = TestSink.create();

		MultiSinkTask<String> multiSinkTask = MultiSinkTask.of(List.of(sink1, sink2));

		Sink<String> sink = multiSinkTask.start();
		sink.send("a");
		latch.await();                  // dispatch of "a" is in progress
		multiSinkTask.registerListener(sink3);  // cannot be applied until that dispatch returns
		sink.send("b");                 // so the next dispatch always sees the addition
		sink.complete();

		multiSinkTask.awaitCompletion();

		assertThat(sink1.getSentItems()).containsExactly("a", "b");
		assertThat(sink2.getSentItems()).containsExactly("a", "b");

		assertThat(sink3.getSentItems()).containsExactly("b");
		assertThat(sink3.isComplete()).isTrue();
		assertThat(sink3.getCompletionException()).isNull();
	}

	@Test
	public void unregisterListenerAfterStart() throws IOException, InterruptedException {
		CountDownLatch latch = new CountDownLatch(1);

		TestSink<String> sink1 = TestSink.<String>create().itemHandler("a", _ -> latch.countDown());
		TestSink<String> sink2 = TestSink.create();

		MultiSinkTask<String> multiSinkTask = MultiSinkTask.of(List.of(sink1, sink2));

		Sink<String> sink = multiSinkTask.start();
		sink.send("a");
		latch.await();
		multiSinkTask.unregisterListener(sink2);
		sink.send("b");
		sink.complete();

		multiSinkTask.awaitCompletion();

		assertThat(sink1.getSentItems()).containsExactly("a", "b");
		assertThat(sink1.getCompletionException()).isNull();

		assertThat(sink2.getSentItems()).containsExactly("a");
		assertThat(sink2.isComplete()).isTrue();
		assertThat(sink2.getCompletionException()).isNull();
	}

	@Test
	public void registerThenUnregisterListenerAfterStart() throws IOException, InterruptedException {
		CountDownLatch latch = new CountDownLatch(1);

		TestSink<String> sink1 = TestSink.<String>create().itemHandler("a", _ -> latch.countDown());
		TestSink<String> sink2 = TestSink.create();

		MultiSinkTask<String> multiSinkTask = MultiSinkTask.of(List.of(sink1));

		Sink<String> sink = multiSinkTask.start();
		sink.send("a");
		latch.await();
		multiSinkTask.registerListener(sink2);     // both staged, applied by the same
		multiSinkTask.unregisterListener(sink2);  // drain: the addition first, then the removal
		sink.send("b");
		sink.complete();

		multiSinkTask.awaitCompletion();

		assertThat(sink1.getSentItems()).containsExactly("a", "b");

		assertThat(sink2.getSentItems()).isEmpty();
		assertThat(sink2.isComplete()).isTrue();
		assertThat(sink2.getCompletionException()).isNull();
	}

	@Test
	public void registerListenerAfterCompletion() throws IOException, InterruptedException {
		TestSink<String> sink1 = TestSink.create();
		TestSink<String> sink2 = TestSink.create();

		MultiSinkTask<String> multiSinkTask = MultiSinkTask.of(List.of(sink1));

		Sink<String> sink = multiSinkTask.start();
		sink.send("a");
		sink.complete();

		multiSinkTask.awaitCompletion();

		multiSinkTask.registerListener(sink2);

		assertThat(sink2.getSentItems()).isEmpty();
		assertThat(sink2.isComplete()).isTrue();
		assertThat(sink2.getCompletionException()).isNull();
	}

	@Test
	public void unregisterLastListenerSkipsDispatch() throws IOException, InterruptedException {
		CountDownLatch latch = new CountDownLatch(1);

		TestSink<String> sink1 = TestSink.<String>create().itemHandler("a", _ -> latch.countDown());

		MultiSinkTask<String> multiSinkTask = MultiSinkTask.of(List.of(sink1));

		Sink<String> sink = multiSinkTask.start();
		sink.send("a");
		latch.await();
		multiSinkTask.unregisterListener(sink1);
		sink.send("b");  // unblocks the task so it can apply the removal
		sink.complete();

		multiSinkTask.awaitCompletion();

		assertThat(sink1.getSentItems()).containsExactly("a");
		assertThat(sink1.isComplete()).isTrue();
		assertThat(sink1.getCompletionException()).isNull();
	}

	@Test
	public void taskStaysAliveWithoutListeners() throws IOException, InterruptedException {
		TestSink<String> sink1 = TestSink.create();

		MultiSinkTask<String> multiSinkTask = MultiSinkTask.of(List.of());

		Sink<String> sink = multiSinkTask.start();
		multiSinkTask.registerListener(sink1);
		sink.send("a");
		sink.complete();

		multiSinkTask.awaitCompletion();

		assertThat(sink1.getSentItems()).containsExactly("a");
		assertThat(sink1.isComplete()).isTrue();
	}

	@Test
	public void startTwice() {
		MultiSinkTask<String> multiSinkTask = MultiSinkTask.of(List.of(TestSink.create()));
		multiSinkTask.start();

		assertThatIllegalStateException().isThrownBy(multiSinkTask::start).withMessage("Already started");
	}


	@Test
	public void completeWhenInterrupted() throws IOException, InterruptedException {
		CountDownLatch latch = new CountDownLatch(1);

		TestSink<String> sink1 = TestSink.create();
		TestSink<String> sink2 = TestSink.create();
		TestSink<String> sink3 = TestSink.<String>create().itemHandler("b", _ -> {
			latch.countDown();
			Thread.sleep(Duration.ofMinutes(30));
		});

		MultiSinkTask<String> multiSinkTask = MultiSinkTask.of(List.of(sink1, sink2, sink3));
		Sink<String> sink = multiSinkTask.start();
		sink.send("a");
		sink.send("b");
		sink.send("c");
		sink.complete();

		latch.await();
		multiSinkTask.cancel();
		multiSinkTask.awaitCompletion();

		assertThat(sink1.getSentItems()).contains("a");
		assertThat(sink2.getSentItems()).contains("a");
		assertThat(sink3.getSentItems()).containsExactly("a", "b");

		assertThat(sink1.getCompletionException()).rootCause().isInstanceOf(InterruptedException.class);
		assertThat(sink2.getCompletionException()).rootCause().isInstanceOf(InterruptedException.class);
		assertThat(sink3.getCompletionException()).rootCause().isInstanceOf(InterruptedException.class);
	}

	@Test
	public void cancelBeforeStartDoesNotHang() {
		try (ExecutorService executor = Executors.newFixedThreadPool(1)) {
			CountDownLatch latch = new CountDownLatch(1);
			executor.submit(() -> {
				latch.await(); // block the single thread, further tasks cannot start
				return null;
			});

			TestSink<String> listener = TestSink.create();
			MultiSinkTask<String> multiSinkTask = MultiSinkTask.of(List.of(listener), executor);
			multiSinkTask.start();

			multiSinkTask.cancel();
			assertTimeoutPreemptively(Duration.ofSeconds(5), multiSinkTask::awaitCompletion);
			latch.countDown();

			assertThat(listener.getCompletionException()).isNotNull();
			assertThat(listener.getSentItems()).isEmpty();
		}
	}

}
