package io.spring.streamsource;

import java.io.IOException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.jspecify.annotations.Nullable;

/**
 * A {@link BlockingQueue}-backed {@link Exchange}.
 *
 * <p>The queue is bounded. Items are received in the order they were sent.
 * When the queue is full, {@link #send} blocks until the consumer makes room,
 * and when it is empty, {@link #receiveNext()} blocks until an item is sent or
 * the Exchange is completed. This blocking is the backpressure between
 * producer and consumer. The capacity is
 * {@value #DEFAULT_CAPACITY} items unless specified otherwise.
 *
 * <p>The {@code Sink} side is for a single producer thread, and the
 * {@code Source} side for a single consumer thread, which must be different
 * threads, see {@link Exchange}. Completion does not block, even when the
 * queue is full.
 *
 * @param <T> the type of items passed through the Exchange
 */
public final class BlockingQueueExchange<T> implements Exchange<T> {

	/**
	 * The capacity used by {@link #BlockingQueueExchange()}.
	 */
	public static final int DEFAULT_CAPACITY = 128;

	private static final Object COMPLETE = new Object();


	private final BlockingQueue<Object> queue;

	private @Nullable T receivedItem;

	private volatile @Nullable Completion completion;

	private volatile boolean closed;


	/**
	 * Create an instance with a capacity of {@value #DEFAULT_CAPACITY} items.
	 */
	public BlockingQueueExchange() {
		this(DEFAULT_CAPACITY);
	}

	/**
	 * Create an instance with the given capacity.
	 * @param capacity the number of items the Exchange can hold before
	 * {@link #send(Object)} blocks, must be greater than zero
	 * @throws IllegalArgumentException if the capacity is not greater than zero
	 */
	public BlockingQueueExchange(int capacity) {
		this.queue = new LinkedBlockingQueue<>(capacity);
	}


	// Sink

	@Override
	public void send(T item) throws InterruptedException {
		if (this.closed) {
			throw new SinkCompleteException("Exchange closed from the Source side");
		}
		Completion c = this.completion;
		if (c != null) {
			throw new SinkCompleteException("Already completed", c.exception());
		}
		try {
			this.queue.put(item);
		}
		catch (InterruptedException ex) {
			completeExceptionally(new RuntimeException("Sending interrupted", ex));
			throw ex;
		}
	}

	/**
	 * Variant of {@link #send} that returns {@code false} without blocking if
	 * there is no remaining capacity.
	 * <p>For internal use by the reactive {@code Publisher} to {@code Source}
	 * bridge to avoid blocking in case of a non-compliant Publisher.
	 * @param item the item to send
	 * @return whether the item was accepted
	 * @throws SinkCompleteException if the Sink was already completed, or
	 * closed from the Source side
	 */
	public boolean trySend(T item) {
		if (this.closed) {
			throw new SinkCompleteException("Exchange closed from the Source side");
		}
		Completion c = this.completion;
		if (c != null) {
			throw new SinkCompleteException("Already completed", c.exception());
		}
		return this.queue.offer(item);
	}

	@Override
	public void complete() {
		completeInternal(null);
	}

	@Override
	public void completeExceptionally(Throwable cause) {
		completeInternal(cause);
	}

	private void completeInternal(@Nullable Throwable cause) {
		if (this.completion != null) {
			return;
		}
		// receiveNext checks the completion field, so
		// offer (rather than put) COMPLETE only to unblock receive side
		this.completion = new Completion(cause);
		this.queue.offer(COMPLETE);
	}


	// Source

	@Override
	public boolean receiveNext() throws IOException, InterruptedException {
		if (this.closed) {
			return returnCompletion();
		}

		if (this.completion != null && this.queue.isEmpty()) {
			closeAfterCompletion();
			return returnCompletion();
		}

		try {
			Object item = this.queue.take();
			setReceivedItem(item);
		}
		catch (InterruptedException ex) {
			close();
			throw ex;
		}

		return (!this.closed);
	}

	private boolean returnCompletion() throws IOException {
		Completion c = this.completion;
		if (c != null) {
			c.throwIfCompletedExceptionally();
		}
		return false;
	}

	@SuppressWarnings("unchecked")
	private void setReceivedItem(@Nullable Object item) throws IOException {
		// (this.receivedItem != null) => discarded item
		if (item == COMPLETE) {
			Completion c = this.completion;
			if (c == null) {
				throw new IllegalStateException("Completion not set");
			}
			closeAfterCompletion();
			c.throwIfCompletedExceptionally();
		}
		else {
			this.receivedItem = (T) item;
		}
	}

	private void closeAfterCompletion() {
		if (this.completion != null && this.queue.isEmpty()) {
			this.closed = true;
		}
	}

	@Override
	public T next() {
		T item = this.receivedItem;
		if (item == null) {
			throw new IllegalStateException("No received item");
		}
		this.receivedItem = null;
		return item;
	}

	@Override
	public void close() {
		this.closed = true;
		this.queue.clear(); // discarded items
	}


	/**
	 * Represents the completion of whatever is sending items to this Exchange.
	 */
	private record Completion(@Nullable Throwable exception) {

		public void throwIfCompletedExceptionally() throws IOException {
			Throwable ex = exception();
			if (ex == null) {
				return;
			}
			if (ex instanceof IOException ioex) {
				throw ioex;
			}
			if (ex instanceof RuntimeException rex) {
				throw rex;
			}
			if (ex instanceof Error e) {
				throw e;
			}
			throw new RuntimeException("Exchange closed with error", ex);
		}
	}

}
