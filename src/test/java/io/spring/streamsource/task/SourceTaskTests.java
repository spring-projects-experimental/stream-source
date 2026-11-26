package io.spring.streamsource.task;


import java.io.IOException;
import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import io.spring.streamsource.Source;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.shadow.de.siegmar.fastcsv.util.Nullable;

import static io.spring.streamsource.TestSourceUtils.toList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalStateException;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;

/**
 * Unit tests for {@link SourceTask}.
 */
public class SourceTaskTests {


	@Test
	public void produceAndReceive() throws IOException, InterruptedException {
		Producer<String> producer = TestProducer.withItems("a", "b");
		try (Source<String> source = startProducer(producer)) {
			assertThat(toList(source)).containsExactly("a", "b");
		}
	}

	@Test
	public void startWorksOnceOnly() {
		SourceTask<String> sourceTask = SourceTask.fromProducer(sink -> {});
		try (Source<String> source = sourceTask.start()) {
			assertThatIllegalStateException().isThrownBy(sourceTask::start).withMessage("Already started");
		}
	}

	@Test
	public void executorCreatesVirtualThreadsByDefault() throws IOException, InterruptedException {
		AtomicBoolean virtual = new AtomicBoolean();
		Producer<String> producer = TestProducer.withProduceStep(sink -> {
			virtual.set(Thread.currentThread().isVirtual());
			sink.send("a");
			sink.send("b");
		});

		try (Source<String> source = startProducer(producer)) {
			assertThat(toList(source)).containsExactly("a", "b");
		}

		assertThat(virtual).isTrue();
	}

	@Test
	public void executorFixedThreadPool() throws IOException, InterruptedException {
		try (ExecutorService executor = Executors.newFixedThreadPool(1)) {
			AtomicBoolean virtual = new AtomicBoolean(true);
			Producer<String> producer = TestProducer.withProduceStep(sink -> {
				virtual.set(Thread.currentThread().isVirtual());
				sink.send("a");
				sink.send("b");
			});

			try (Source<String> source = startProducer(producer, executor)) {
				assertThat(toList(source)).containsExactly("a", "b");
			}

			assertThat(virtual).isFalse();
		}
	}

	@Test
	public void closeBeforeProducerStartsDoesNotHang() throws Exception {
		try (ExecutorService executor = Executors.newFixedThreadPool(1)) {
			CountDownLatch latch = new CountDownLatch(1);
			executor.submit(() -> {
				latch.await(); // block the single thread, further tasks cannot start
				return null;
			});

			AtomicBoolean produced = new AtomicBoolean();
			Producer<String> producer = TestProducer.withProduceStep(sink -> produced.set(true));
			Source<String> source = startProducer(producer, executor);

			assertTimeoutPreemptively(Duration.ofSeconds(5), source::close);
			latch.countDown();

			executor.shutdown();
			assertThat(executor.awaitTermination(5, TimeUnit.SECONDS)).isTrue();
			assertThat(produced).isFalse();
		}
	}


	@Test
	public void interruptedSendDoesNotFailOtherSenders() throws Exception {
		CountDownLatch firstItemSent = new CountDownLatch(1);
		CountDownLatch cancelledTaskDone = new CountDownLatch(1);
		AtomicReference<Throwable> cancelledTaskFailure = new AtomicReference<>();

		Producer<String> producer = sink -> {
			try (ExecutorService executor = Executors.newFixedThreadPool(2)) {
				Future<?> cancelledTask = executor.submit(() -> {
					try {
						sink.send("a");
						firstItemSent.countDown();
						sink.send("b"); // blocks: capacity 1, and the consumer is not receiving yet
					}
					catch (Throwable ex) {
						cancelledTaskFailure.set(ex);
					}
					finally {
						cancelledTaskDone.countDown();
					}
				});
				firstItemSent.await();
				cancelledTask.cancel(true);
				cancelledTaskDone.await();

				executor.submit(() -> {
					sink.send("c");
					sink.send("d");
					return null;
				}).get();
			}
		};

		try (Source<String> source = SourceTask.fromProducer(producer).capacity(1).start()) {
			cancelledTaskDone.await();
			assertThat(toList(source)).containsExactly("a", "c", "d");
		}

		assertThat(cancelledTaskFailure.get()).isInstanceOf(InterruptedException.class);
	}


	private static <T> Source<T> startProducer(Producer<T> producer) {
		return startProducer(producer, null);
	}

	private static <T> Source<T> startProducer(Producer<T> producer, @Nullable ExecutorService executor) {
		SourceTask<T> sourceTask = (executor != null ?
				SourceTask.fromProducer(producer, executor) : SourceTask.fromProducer(producer));
		return sourceTask.start();
	}

}
