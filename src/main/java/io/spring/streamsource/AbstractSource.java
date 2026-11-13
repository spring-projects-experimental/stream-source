package io.spring.streamsource;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicReference;

import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Convenient base class for {@link Source} implementations that pre-implements
 * the receiving loop, exception handling, and lifecycle management.
 *
 * <p>Subclasses implement {@link #receiveItem()} for the logic to produce each
 * item, and optionally {@link #handleClose} to clean up underlying resources.
 * Items are never {@code null}, since {@code null} from {@code receiveItem()}
 * means the end of the stream.
 *
 * <p>An example {@code Source} of lines from a {@code BufferedReader}:
 * <pre class="code">
 * class LineSource extends AbstractSource&lt;String&gt; {
 *
 *     private final BufferedReader reader;
 *
 *     LineSource(BufferedReader reader) {
 *         this.reader = reader;
 *     }
 *
 *     &#64;Override
 *     protected &#64;Nullable String receiveItem() throws IOException {
 *         return this.reader.readLine(); // null at the end
 *     }
 *
 *     &#64;Override
 *     protected void handleClose(&#64;Nullable Throwable failure) throws IOException {
 *         this.reader.close();
 *     }
 * }
 * </pre>
 *
 * @param <T> the type of items received from the Source
 */
public abstract class AbstractSource<T> implements Source<T> {

	/**
	 * Logger for use in subclasses, named after the concrete class.
	 */
	protected final Logger logger = LoggerFactory.getLogger(getClass());


	private @Nullable T receivedItem;

	private final AtomicReference<@Nullable Object> closure = new AtomicReference<>(); // Boolean.TRUE | IOException | RuntimeException


	/**
	 * Protected constructor for subclasses.
	 */
	protected AbstractSource() {
	}


	@Override
	public final boolean receiveNext() throws IOException, InterruptedException {
		Object closure = this.closure.get();
		if (closure != null) {
			throwIfClosedWithError(closure);
			return false;
		}
		logger.debug("Receiving next..");
		try {
			T item = receiveItem();
			if (item == null) {
				logger.debug("No more items to receive");
				close();
				return false;
			}
			// (this.receivedItem != null) => discarded item
			logger.debug("received {}", item);
			this.receivedItem = item;
			return true;
		}
		catch (IOException | InterruptedException | RuntimeException | Error ex) {
			closeInternal(ex);
			throw ex;
		}
		catch (Throwable ex) {
			// should never happen, no checked exceptions from receiveItem
			RuntimeException wrapped = wrapException(ex);
			closeInternal(wrapped);
			throw wrapped;
		}
	}

	private static void throwIfClosedWithError(Object closure) throws IOException {
		if (closure instanceof IOException ex) {
			throw ex;
		}
		if (closure instanceof RuntimeException ex) {
			throw ex;
		}
		if (closure instanceof Error ex) {
			throw ex;
		}
		if (closure instanceof Throwable ex) {
			// should never happen, already wrapped in receiveNext on failure
			throw wrapException(ex);
		}
	}

	private static RuntimeException wrapException(Throwable ex) {
		return new RuntimeException("Source closed with error", ex);
	}

	/**
	 * Implement this method to actually retrieve the next item, blocking until a
	 * complete item is available or the end of the stream is reached.
	 * <p>This method is called from {@link #receiveNext()} on the receiving
	 * thread, and never after the Source is closed, whether by {@link #close()},
	 * at the end of the stream, or after a failure.
	 * <p>Block until an item is available, and return {@code null} at the end
	 * of the stream, which closes the Source.
	 * <p>Any exception thrown is terminal. The Source is closed, the exception
	 * is passed to {@link #handleClose}, and it is rethrown from
	 * {@code receiveNext()}. Do not swallow an {@code InterruptedException}.
	 * Interrupting the receiving thread is how a blocked receive is cancelled,
	 * see <a href="https://github.com/spring-projects-experimental/stream-source/blob/main/CANCELLATION.md">CANCELLATION.md</a>.
	 * @return the next item, or {@code null} if there are no more items to receive
	 * @throws IOException if the receive encounters an I/O error
	 * @throws InterruptedException if the receiving thread is interrupted
	 */
	protected abstract @Nullable T receiveItem() throws IOException, InterruptedException;

	@Override
	public T next() {
		T item = this.receivedItem;
		if (item == null) {
			throw new IllegalStateException("No received item");
		}
		this.receivedItem = null;
		return item;
	}

	/**
	 * {@inheritDoc}
	 * <p>This method is final, and idempotent. Only the first close, whether
	 * through this method, at the end of the stream, or after a failure, calls
	 * {@link #handleClose}, which subclasses can override for resource cleanup.
	 * Exceptions from {@code handleClose} are logged at debug level and not
	 * propagated.
	 */
	@Override
	public final void close() {
		// (this.receivedItem != null) => discarded item
		closeInternal(null);
	}

	private void closeInternal(@Nullable Throwable ex) {
		// An interrupt concerns the receiving thread, not the Source, so it is thrown only once
		Object closure = (ex != null && !(ex instanceof InterruptedException) ? ex : Boolean.TRUE);
		if (!this.closure.compareAndSet(null, closure)) {
			return;
		}

		if (ex != null) {
			logger.debug("{} closed due to a failure while receiving", getClass().getSimpleName(), ex);
		}
		else {
			logger.debug("{} closed", getClass().getSimpleName());
		}

		try {
			handleClose(ex);
		}
		catch (Throwable ex2) {
			logger.debug("Error while handling close", ex2);
		}
	}

	/**
	 * Subclasses can override this method to perform resource cleanup when the
	 * {@code Source} is closed via {@link #close()}, at the end of the stream,
	 * or as a result of a failure in {@link #receiveItem()}.
	 * <p>This method is called exactly once on the thread that closes the Source.
	 * The default implementation does nothing.
	 * @param failure {@code null} if the Source was closed via {@link #close()}
	 * or at the end of the stream, otherwise it's the exception thrown by
	 * {@link #receiveItem()}, including {@code InterruptedException}
	 * @throws IOException if cleanup fails
	 */
	protected void handleClose(@Nullable Throwable failure) throws IOException {
	}

}
