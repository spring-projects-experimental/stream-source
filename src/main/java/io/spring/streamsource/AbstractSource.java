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
 */
public abstract class AbstractSource<T> implements Source<T> {

	protected final Logger logger = LoggerFactory.getLogger(getClass());


	private @Nullable T receivedItem;

	private final AtomicReference<@Nullable Object> closure = new AtomicReference<>(); // Boolean.TRUE | IOException | RuntimeException


	protected AbstractSource() {
	}


	@Override
	public boolean receiveNext() throws IOException, InterruptedException {
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
	 * Implement this method to actually retrieve the next item.
	 * @return the next item, or {@code null} if there are no more items to receive
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
	 * <p>Subclasses can overrride {@link #handleClose} for resource cleanup.
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
	 * {@code Source} is closed via {@link #close()}, or as a result of a
	 * failure in {@link #receiveItem()}.
	 * <p>This method is called at most once.
	 * @param failure an underlying cause for the close, if any
	 */
	protected void handleClose(@Nullable Throwable failure) throws IOException {
	}

}
