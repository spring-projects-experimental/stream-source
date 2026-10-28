package io.spring.streamsource;


import java.io.IOException;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.assertj.core.api.Assertions.assertThatIllegalStateException;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Unit tests for {@link BlockingQueueExchange}.
 *
 * <p>Tests in this class are aligned with {@link AbstractSourceTests} for the
 * receiving side. Methods names and assertion details may vary slightly, but
 * test logic and expectations should be the same. New tests here should be
 * added to both tests classes to maintain the equivalence.
 */
@SuppressWarnings("UnnecessaryLocalVariable")
public class BlockingQueueExchangeTests {


	@SuppressWarnings("ResultOfMethodCallIgnored")
	@AfterEach
	void tearDown() {
		Thread.interrupted();
	}


	@Test
	public void sendAndReceive() throws IOException, InterruptedException {
		try (BlockingQueueExchange<String> exchange = new BlockingQueueExchange<>()) {
			Sink<String> sink = exchange;
			Source<String> source = exchange;

			sink.send("item 1");

			assertThat(source.receiveNext()).isTrue();
			assertThat(source.next()).isEqualTo("item 1");
		}
	}

	@Test
	public void sendAndComplete() throws IOException, InterruptedException {
		try (BlockingQueueExchange<String> exchange = new BlockingQueueExchange<>()) {
			Sink<String> sink = exchange;
			Source<String> source = exchange;

			sink.send("item 1");
			sink.send("item 2");
			sink.complete();

			assertThat(source.receiveNext()).isTrue();
			assertThat(source.next()).isEqualTo("item 1");

			assertThat(source.receiveNext()).isTrue();
			assertThat(source.next()).isEqualTo("item 2");

			assertThat(source.receiveNext()).isFalse();
		}
	}

	@Test
	public void sendAndCompleteExceptionally() throws IOException, InterruptedException {
		try (BlockingQueueExchange<String> exchange = new BlockingQueueExchange<>()) {
			Sink<String> sink = exchange;
			Source<String> source = exchange;

			sink.send("item 1");
			sink.send("item 2");
			sink.completeExceptionally(new IllegalStateException("boom"));

			assertThat(source.receiveNext()).isTrue();
			assertThat(source.next()).isEqualTo("item 1");

			assertThat(source.receiveNext()).isTrue();
			assertThat(source.next()).isEqualTo("item 2");

			assertThatIllegalStateException().isThrownBy(source::receiveNext).withMessage("boom");
		}
	}

	@Test
	public void sendAndCompleteWithIOException() {
		try (BlockingQueueExchange<String> exchange = new BlockingQueueExchange<>()) {
			Sink<String> sink = exchange;
			Source<String> source = exchange;

			IOException ex = new IOException("boom");
			sink.completeExceptionally(ex);

			assertThatThrownBy(source::receiveNext).isSameAs(ex);
		}
	}

	@Test
	public void sendAndCompleteWithRuntimeException() {
		try (BlockingQueueExchange<String> exchange = new BlockingQueueExchange<>()) {
			Sink<String> sink = exchange;
			Source<String> source = exchange;

			RuntimeException ex = new RuntimeException("boom");
			sink.completeExceptionally(ex);

			assertThatThrownBy(source::receiveNext).isSameAs(ex);
		}
	}

	@Test
	public void sendAndCompleteWithError() {
		try (BlockingQueueExchange<String> exchange = new BlockingQueueExchange<>()) {
			Sink<String> sink = exchange;
			Source<String> source = exchange;

			Error ex = new Error("boom");
			sink.completeExceptionally(ex);

			assertThatThrownBy(source::receiveNext).isSameAs(ex);
		}
	}

	@Test
	public void sendAndCompleteWithException() {
		try (BlockingQueueExchange<String> exchange = new BlockingQueueExchange<>()) {
			Sink<String> sink = exchange;
			Source<String> source = exchange;

			Exception ex = new Exception("boom");
			sink.completeExceptionally(ex);

			assertThatThrownBy(source::receiveNext)
					.isInstanceOf(RuntimeException.class).hasMessage("Exchange closed with error")
					.rootCause().hasMessage("boom").isSameAs(ex);
		}
	}

	@Test
	public void sendNotAllowedAfterComplete() throws IOException, InterruptedException {
		try (Exchange<String> sink = new BlockingQueueExchange<>()) {
			sink.send("item 1");
			sink.complete();

			assertThatThrownBy(() -> sink.send("item 2"))
					.isInstanceOf(SinkCompleteException.class).hasMessage("Already completed");
		}
	}

	@Test
	public void sendNotAllowedAfterCompleteExceptionally() throws IOException, InterruptedException {
		try (Exchange<String> sink = new BlockingQueueExchange<>()) {
			sink.send("item 1");
			sink.completeExceptionally(new IllegalStateException("boom"));

			assertThatThrownBy(() -> sink.send("item 2"))
					.isInstanceOf(SinkCompleteException.class).hasMessage("Already completed")
					.hasRootCauseMessage("boom");
		}
	}

	@Test
	public void sendNotAllowedAfterClose() throws IOException, InterruptedException {
		BlockingQueueExchange<String> exchange = new BlockingQueueExchange<>();
		Sink<String> sink = exchange;
		Source<String> source = exchange;

		sink.send("item 1");
		source.close();

		assertThatThrownBy(() -> exchange.send("item 2"))
				.isInstanceOf(SinkCompleteException.class).hasMessage("Exchange closed from the Source side");

		assertThat(exchange.receiveNext()).isFalse();
	}

	@Test
	public void receiveAfterCloseRethrows() {
		try (BlockingQueueExchange<String> exchange = new BlockingQueueExchange<>()) {
			Sink<String> sink = exchange;
			Source<String> source = exchange;

			IOException ex = new IOException("boom");
			sink.completeExceptionally(ex);

			assertThatThrownBy(source::receiveNext).isSameAs(ex);
			assertThatThrownBy(source::receiveNext).as("should rethrow any number of times").isSameAs(ex);
		}
	}

	@Test
	void receiveMultipleTimesSkips() throws IOException, InterruptedException {
		try (BlockingQueueExchange<String> exchange = new BlockingQueueExchange<>()) {
			Sink<String> sink = exchange;
			Source<String> source = exchange;

			sink.send("item 1");
			sink.send("item 2");
			sink.complete();

			assertThat(source.receiveNext()).isTrue();
			assertThat(source.receiveNext()).isTrue();
			assertThat(source.next()).isEqualTo("item 2");

			assertThat(source.receiveNext()).isFalse();
		}
	}

	@Test
	void receiveMultipleTimesHandlesCompletion() throws IOException, InterruptedException {
		try (BlockingQueueExchange<String> exchange = new BlockingQueueExchange<>()) {
			Sink<String> sink = exchange;
			Source<String> source = exchange;

			sink.send("item 1");
			sink.send("item 2");
			sink.complete();

			assertThat(source.receiveNext()).isTrue();
			assertThat(source.receiveNext()).isTrue();
			assertThat(source.receiveNext()).isFalse();
		}
	}

	@Test
	void nextRequiresReceive() throws IOException, InterruptedException {
		try (BlockingQueueExchange<String> exchange = new BlockingQueueExchange<>()) {
			Sink<String> sink = exchange;
			Source<String> source = exchange;

			sink.send("item 1");
			sink.send("item 2");
			sink.complete();

			assertThatIllegalStateException().isThrownBy(source::next).withMessage("No received item");
		}
	}

	@Test
	void interruptedSendCompletesExceptionally() {
		try (BlockingQueueExchange<String> exchange = new BlockingQueueExchange<>()) {
			Sink<String> sink = exchange;
			Source<String> source = exchange;

			Thread.currentThread().interrupt();

			assertThatExceptionOfType(InterruptedException.class).isThrownBy(() -> sink.send("item 1"));
			assertThatExceptionOfType(RuntimeException.class)
					.isThrownBy(source::receiveNext).withMessage("Sending interrupted");
		}
	}

	@Test
	void interruptedCompletionHasNoSideEffects() throws IOException, InterruptedException {
		try (BlockingQueueExchange<String> exchange = new BlockingQueueExchange<>()) {
			Sink<String> sink = exchange;
			Source<String> source = exchange;

			Thread.currentThread().interrupt();

			sink.complete();
			assertThat(Thread.currentThread().isInterrupted()).isTrue();

			Thread.interrupted();

			assertThat(source.receiveNext()).isFalse();
		}
	}

	@Test
	void interruptedReceiveCloses() throws IOException, InterruptedException {
		try (BlockingQueueExchange<String> exchange = new BlockingQueueExchange<>()) {
			Sink<String> sink = exchange;
			Source<String> source = exchange;

			sink.send("item 1");
			sink.complete();

			Thread.currentThread().interrupt();

			assertThatExceptionOfType(InterruptedException.class).isThrownBy(source::receiveNext);
			assertThat(source.receiveNext()).as("should not rethrow the interrupt").isFalse();
		}
	}

	@Test
	public void close() throws IOException, InterruptedException {
		try (BlockingQueueExchange<String> exchange = new BlockingQueueExchange<>()) {
			Sink<String> sink = exchange;
			Source<String> source = exchange;

			sink.send("item 1");
			sink.send("item 1");

			source.close();

			assertThat(source.receiveNext()).isFalse();
		}
	}

	@Test
	void completeMultipleTimesIgnored() throws IOException, InterruptedException {
		try (BlockingQueueExchange<String> exchange = new BlockingQueueExchange<>()) {
			Sink<String> sink = exchange;
			Source<String> source = exchange;

			sink.complete();
			sink.completeExceptionally(new IllegalStateException("boom"));

			assertThat(source.receiveNext()).as("should not raise exception").isFalse();
		}
	}
}
