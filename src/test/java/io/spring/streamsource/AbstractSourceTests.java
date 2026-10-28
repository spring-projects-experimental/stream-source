package io.spring.streamsource;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIOException;
import static org.assertj.core.api.Assertions.assertThatIllegalStateException;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests for {@link AbstractSource}.
 *
 * <p>Tests in this class are aligned with {@link BlockingQueueExchangeTests} for
 * receiving logic. Methods names and assertion details may vary slightly, but
 * test logic and expectations should be the same. New tests here should be
 * added to both tests classes to maintain the equivalence.
 */
@SuppressWarnings("resource")
class AbstractSourceTests {


	@Test
	void receive() throws IOException, InterruptedException {
		TestSource<String> source = TestSource.withItems("item 1");

		assertThat(source.receiveNext()).isTrue();
		assertThat(source.next()).isEqualTo("item 1");
	}

	@Test
	void receiveToEnd() throws IOException, InterruptedException {
		TestSource<String> source = TestSource.withItems("item 1", "item 2");

		assertThat(source.receiveNext()).isTrue();
		assertThat(source.next()).isEqualTo("item 1");

		assertThat(source.receiveNext()).isTrue();
		assertThat(source.next()).isEqualTo("item 2");

		assertThat(source.receiveNext()).isFalse();
	}

	@Test
	void receiveToException() throws IOException, InterruptedException {
		Source<String> source = TestSource.withItems("item 1", "item 2").failure(new IllegalStateException("boom"));

		assertThat(source.receiveNext()).isTrue();
		assertThat(source.next()).isEqualTo("item 1");

		assertThat(source.receiveNext()).isTrue();
		assertThat(source.next()).isEqualTo("item 2");

		assertThatIllegalStateException().isThrownBy(source::receiveNext).withMessage("boom");
	}

	@Test
	void receiveToIOException() {
		IOException ex = new IOException("simulated I/O error");
		Source<String> source = TestSource.withFailure(ex);

		assertThatIOException().isThrownBy(source::receiveNext).isSameAs(ex);
	}

	@Test
	void receiveToInterruptedException() throws IOException, InterruptedException {
		InterruptedException ex = new InterruptedException("simulated interrupt");
		Source<String> source = TestSource.withFailure(ex);

		assertThatThrownBy(source::receiveNext).isSameAs(ex);
		assertThat(source.receiveNext()).as("should not rethrow the interrupt").isFalse();
	}

	@Test
	void receiveToRuntimeException() {
		RuntimeException ex = new RuntimeException("boom");
		Source<String> source = TestSource.withFailure(ex);

		assertThatThrownBy(source::receiveNext).isSameAs(ex);
	}

	@Test
	void receiveToError() {
		Error ex = new Error("boom");
		Source<String> source = TestSource.withFailure(ex);

		assertThatThrownBy(source::receiveNext).isSameAs(ex);
	}

	@Test
	void receiveAfterCloseRethrows() {
		IOException ex = new IOException("boom");
		Source<String> source = TestSource.withFailure(ex);

		assertThatThrownBy(source::receiveNext).isSameAs(ex);

		assertThatThrownBy(source::receiveNext).isSameAs(ex);
	}

	@Test
	void receiveMultipleTimesSkips() throws IOException, InterruptedException {
		TestSource<String> source = TestSource.withItems("item 1", "item 2");

		assertThat(source.receiveNext()).isTrue();
		assertThat(source.receiveNext()).isTrue();
		assertThat(source.next()).isEqualTo("item 2");

		assertThat(source.receiveNext()).isFalse();
	}

	@Test
	void nextRequiresReceive() {
		TestSource<String> source = TestSource.withItems("item 1", "item 2");

		assertThatIllegalStateException().isThrownBy(source::next).withMessage("No received item");
	}

	@Test
	void close() throws IOException, InterruptedException {
		TestSource<String> source = TestSource.withItems("item 1", "item 2");
		source.close();

		assertThat(source.receiveNext()).isFalse();
	}

	@Test
	void closeHandlerIsCalledOnce() throws Exception {
		int threadCount = 8;
		ExecutorService executor = Executors.newFixedThreadPool(threadCount);
		try {
			for (int round = 0; round < 2000; round++) {
				CloseCountingSource<String> source = new CloseCountingSource<>();
				CountDownLatch latch = new CountDownLatch(1);
				List<Future<?>> futures = new ArrayList<>(threadCount);
				for (int i = 0; i < threadCount; i++) {
					futures.add(executor.submit(() -> {
						latch.await();
						source.close();
						return null;
					}));
				}
				latch.countDown();
				for (Future<?> future : futures) {
					future.get(2, TimeUnit.SECONDS);
				}
				assertThat(source.getCount()).as("Round %d", round).isEqualTo(1);
			}
		}
		finally {
			executor.shutdownNow();
		}
	}


	private static class CloseCountingSource<T> extends AbstractSource<T> {

		private final AtomicInteger count = new AtomicInteger();

		public int getCount() {
			return this.count.get();
		}

		@Override
		protected @Nullable T receiveItem() {
			return null;
		}

		@Override
		protected void handleClose(@Nullable Throwable failure) {
			this.count.incrementAndGet();
		}
	}

}
