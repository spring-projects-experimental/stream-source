package io.spring.streamsource;

import java.io.IOException;

/**
 * Contract to receive a stream of items in an imperative loop. It has the
 * simplicity of {@link java.util.Iterator}, but for items that may be received
 * from a remote host, with a lifecycle and possible network failures.
 *
 * <p>Call {@link #receiveNext()} to receive an item, and {@link #next()} to
 * access it. The stream ends when {@code receiveNext()} returns {@code false}
 * or when the consumer calls {@link #close()}. A {@code Source} holds
 * underlying resources and is intended to use in try-with-resources:
 *
 * <pre class="code">
 * try (Source&lt;String&gt; source = ...) {
 *     while (source.receiveNext()) {
 *         String item = source.next();
 *         // process item (never null)
 *     }
 * }
 * </pre>
 *
 * <p>{@code Source} is generally for use from a single thread, and that means
 * calling {@link #receiveNext()} and {@link #next()} from one thread. A blocked
 * {@link #receiveNext()} may be cancelled by interrupting the receiving thread,
 * see <a href="https://github.com/spring-projects-experimental/stream-source/blob/main/CANCELLATION.md">CANCELLATION.md</a>.
 * In addition, the {@link #close()} method may be called from another thread.
 *
 * <p>To implement a {@code Source}, extend {@link AbstractSource}.
 * For an independent producer-consumer hand-off, see {@link Exchange}.
 *
 * @see AbstractSource
 * @see Exchange
 *
 * @param <T> the type of items received from the Source
 */
public interface Source<T> extends AutoCloseable {

	/**
	 * Receive the next item from the Source, blocking until a complete item is
	 * available or the end of the stream is reached.
	 * <p>A return value of {@code true} means an item was received, and
	 * {@link #next()} will return it. If called again without {@code next()},
	 * this method receives the next item and discards the current one.
	 * <p>A return value of {@code false} means the Source has ended, or was
	 * {@link #close() closed} and will not return more items. If this method
	 * fails, the Source is closed and cannot receive more items. Once a Source
	 * ends, subsequent calls produce the same result without blocking, either
	 * a {@code false} return value or the same exception.
	 * @return {@code true} if an item was successfully received, or
	 * {@code false} if the Source has ended.
	 * @throws IOException if the receive encounters an I/O error
	 * @throws InterruptedException if the receiving thread is interrupted
	 * @throws RuntimeException if the receive fails for any other reason
	 */
	boolean receiveNext() throws IOException, InterruptedException;

	/**
	 * Return the item received by the last call to {@link #receiveNext()}.
	 * Each item can be retrieved only once, and the call must be preceded by a
	 * call to {@code receiveNext()} that returned {@code true}.
	 * <p>The result of calling this method after {@link #close()} is
	 * unspecified.
	 * @return the received item, never {@code null}
	 * @throws IllegalStateException if the call was not preceded by a call to
	 * {@code receiveNext()} that returned {@code true}, or if the item was
	 * already retrieved
	 */
	T next();

	/**
	 * Close the Source to release underlying resources such as a connection.
	 * Subsequent calls to {@link #receiveNext()} return {@code false}, and
	 * {@link #next()} cannot be relied on to return a received item.
	 * <p>This method is idempotent, and does not throw checked exceptions.
	 * Failures to release resources are not propagated.
	 * <p>The preferred way to cancel a blocked {@link #receiveNext()} is to
	 * interrupt the receiving thread. If necessary, {@code close()} may be called
	 * from another thread, currently supported for subclasses of
	 * {@link AbstractSource}, but not by {@link BlockingQueueExchange}.
	 * <p>Closing an {@link Exchange} from the Source side discards items that
	 * were sent but not yet received, and causes further calls to
	 * {@link Sink#send} to fail with {@link SinkCompleteException}.
	 */
	@Override
	void close();

}
