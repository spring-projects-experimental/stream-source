package io.spring.streamsource.task;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;

import io.spring.streamsource.BlockingQueueExchange;
import io.spring.streamsource.Exchange;
import io.spring.streamsource.Sink;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A helper class create a {@link Sink} to fan out items to a list of {@code Sink}
 * listeners efficiently and without holding up the producer pushing into the sink
 * while also applying backpressure if necessary.
 *
 * <p>This is done by putting items into an {@link Exchange}, starting a background
 * thread to receive from the exchange, dispatching every item to every listener
 * concurrently, and waiting for all listeners to finish before moving on to the
 * next item.
 *
 * <p>Listeners can be {@link #registerListener(Sink) registered} and
 * {@link #unregisterListener(Sink) unregistered} dynamically at any time.
 * A listener that raises an exception is completed and removed from the list.
 *
 * <p>Use static factory methods to create a {@code MultiSink}, and call
 * {@link #start()} to launch the fanout task and obtain the {@code Sink} to
 * push items into.
 *
 * <p>The fanout task ends when the outer {@link Sink} is completed and all items
 * are drained. Completing it is up to the caller in any outcome. You can also
 * {@link #cancel() cancel} to interrupt the fanout task and complete all
 * listeners immediately. Use {@link #awaitCompletion()} to wait for the
 * background task to end.
 *
 * <p>By default, listeners are notified on virtual threads, which requires
 * Java 21+. On Java &lt; 21, use the factory method accepting an
 * {@link ExecutorService}.
 *
 * @param <T> the type of items passed to the listeners
 */
public final class MultiSink<T> {

	private static final Logger logger = LoggerFactory.getLogger(MultiSink.class);

	private static final @Nullable ExecutorService defaultExecutorService = createDefaultExecutorService();


	private final ListenerRegistry<T> listenerRegistry;

	private final ExecutorService executorService;

	private @Nullable Integer capacity;

	private volatile @Nullable FanOutTask task;

	private volatile @Nullable Future<?> future;


	private MultiSink(List<Sink<T>> listeners, ExecutorService executorService) {
		this.listenerRegistry = new ListenerRegistry<>(listeners);
		this.executorService = executorService;
	}


	/**
	 * Set the capacity for the internal {@link Exchange} where items are first
	 * pushed into to decouple the outer producer from the listeners.
	 * @param capacity the capacity to use
	 * @return the same instance for chaining
	 */
	public MultiSink<T> capacity(int capacity) {
		this.capacity = capacity;
		return this;
	}

	/**
	 * Register a new {@code Sink} listener. Listener additions are staged and
	 * applied before each dispatch.
	 * <p>Listeners added after the fanout task ends are completed.
	 * @param listener the listener to register
	 */
	public void registerListener(Sink<T> listener) {
		this.listenerRegistry.add(listener);
		FanOutTask task = this.task;
		if (task != null && task.isDone()) {
			this.listenerRegistry.completeStagedAdditions();
		}
	}

	/**
	 * Unregister a {@code Sink} listener. Listener removals are staged and
	 * applied before each dispatch.
	 * @param listener the listener to unregister
	 */
	public void unregisterListener(Sink<T> listener) {
		this.listenerRegistry.remove(listener);
	}


	/**
	 * Submit the fanout task through the configured {@code ExecutorService}
	 * to start receiving items pushed into the returned {@code Sink}, and
	 * dispatching them to all listeners.
	 * @return a {@code Sink} to push items into
	 * @throws IllegalStateException if already started
	 */
	public Sink<T> start() {
		if (this.task != null) {
			throw new IllegalStateException("Already started");
		}
		BlockingQueueExchange<T> exchange = initExchange();
		FanOutTask task = new FanOutTask(exchange);
		this.task = task;
		this.future = this.executorService.submit(task);
		return exchange;
	}

	private BlockingQueueExchange<T> initExchange() {
		return (this.capacity != null ?
				new BlockingQueueExchange<>(this.capacity) : new BlockingQueueExchange<>());
	}

	/**
	 * Cancel the fanout task and return immediately interrupting dispatches to
	 * individual listeners and discarding any queued items.
	 * <p>For an orderly end instead, complete the {@code Sink} returned from
	 * {@link #start()}, and the background task will end once all queued items
	 * have been processed.
	 * <p>Use {@link #awaitCompletion()} to wait until the fanout task has
	 * actually ended.
	 */
	public void cancel() {
		Future<?> future = this.future;
		if (future == null) {
			throw new IllegalStateException("Not started");
		}
		future.cancel(true);

		FanOutTask task = this.task;
		if (task != null) {
			task.cancelIfNotStarted();
		}
	}

	/**
	 * Wait for the background dispatching task to finish, which is when the
	 * {@code Sink} returned from {@link #start()} is complete and all items
	 * have been processed, or the fanout task has been {@link #cancel() cancelled}.
	 * @throws IllegalStateException if {@link #start()} was not called
	 * @throws InterruptedException if the current thread is interrupted while waiting
	 */
	public void awaitCompletion() throws InterruptedException {
		FanOutTask task = this.task;
		if (task == null) {
			throw new IllegalStateException("Not started, call start() first");
		}
		task.awaitCompletion();
	}


	/**
	 * Static factory method for a {@code FanOutTask} instance with a list of
	 * {@code Sink} listeners to fan out items to.
	 * <p>Requires Java 21+. Use {@link #of(List, ExecutorService)} on lower versions.
	 * @param sinks the listeners to fan out to
	 * @return the created instance
	 * @param <T> the type of items produced
	 * @throws IllegalStateException if virtual threads are not supported
	 */
	public static <T> MultiSink<T> of(List<Sink<T>> sinks) {
		ExecutorService executorService = defaultExecutorService;
		if (executorService == null) {
			throw new IllegalStateException(
					"Virtual threads require Java 21+, " +
							"use the alternative factory method that accepts an ExecutorService");
		}
		return of(sinks, executorService);
	}

	/**
	 * Alternative to {@link #of(List)} with a given {@code ExecutorService}.
	 * {@code Sink} listeners to fan out items to.
	 * @param executorService the executor service used both to run the background
	 * fanout task, and the threads to notify each listener; can be used to
	 * provide a non-virtual threads {@code ExecutorService} (e.g. a thread pool
	 * with platform threads), compatible with Java &lt; 21. The caller is
	 * responsible for managing its lifecycle.
	 * @return the created instance
	 * @param <T> the type of items produced
	 */
	public static <T> MultiSink<T> of(List<Sink<T>> sinks, ExecutorService executorService) {
		return new MultiSink<>(sinks, executorService);
	}


	private static @Nullable ExecutorService createDefaultExecutorService() {
		try {
			return new TaskVirtualThreadDelegate().newVirtualThreadPerTaskExecutor();
		}
		catch (UnsupportedOperationException ex) {
			return null;
		}
	}


	/**
	 * Fanout task to read from an {@code Exchange} and dispatch each item to all
	 * listeners concurrently, wait for all to finish with a given item. Listener
	 * additions and removals are applied between dispatches.
	 */
	private final class FanOutTask implements Runnable {

		private final Exchange<T> exchange;

		private final CountDownLatch completionLatch = new CountDownLatch(1);

		private final AtomicBoolean running = new AtomicBoolean();

		private FanOutTask(Exchange<T> exchange) {
			this.exchange = exchange;
		}

		public boolean isDone() {
			return (this.completionLatch.getCount() == 0);
		}

		@Override
		public void run() {
			if (!claimStart()) {
				return;
			}
			ListenerRegistry<T> registry = MultiSink.this.listenerRegistry;
			Throwable cause = null;
			try {
				registry.applyChanges();
				while (this.exchange.receiveNext()) {
					T item = this.exchange.next();
					if (registry.applyChanges()) {
						dispatch(item);
					}
				}
			}
			catch (InterruptedException ex) {
				cause = new RuntimeException("FanOutTask interrupted", ex);
			}
			catch (Throwable ex) {
				cause = ex;
			}
			finally {
				finish(cause);
			}
		}

		private boolean claimStart() {
			return this.running.compareAndSet(false, true);
		}

		private void dispatch(T item) throws InterruptedException {
			List<Sink<T>> listeners = MultiSink.this.listenerRegistry.listeners();
			List<Future<@Nullable Void>> futures = new ArrayList<>(listeners.size());
			for (Sink<T> sink : listeners) {
				futures.add(MultiSink.this.executorService.submit(() -> {
					sink.send(item);
					return null;
				}));
			}
			Iterator<Sink<T>> iterator = listeners.iterator();
			for (Future<@Nullable Void> future : futures) {
				Sink<T> sink = iterator.next();
				try {
					future.get();
				}
				catch (ExecutionException ex) {
					Throwable cause = ex.getCause();
					logger.debug("Removing Sink after failure to send: {}", sink, cause);
					iterator.remove();
					sink.completeExceptionally(cause != null ? cause : ex);
				}
				catch (InterruptedException ex) {
					Thread.currentThread().interrupt();
					iterator.remove();
					sink.completeExceptionally(new RuntimeException("MultiSink send interrupted", ex));
				}
				if (Thread.interrupted()) {
					throw new InterruptedException();
				}
			}
		}

		private void finish(@Nullable Throwable cause) {
			completeRemainingListeners(cause);
			this.exchange.close();
			this.completionLatch.countDown();
			MultiSink.this.listenerRegistry.completeStagedAdditions();
		}

		private void completeRemainingListeners(@Nullable Throwable cause) {
			for (Sink<T> sink : MultiSink.this.listenerRegistry.listeners()) {
				try {
					if (cause != null) {
						sink.completeExceptionally(cause);
					}
					else {
						sink.complete();
					}
				}
				catch (RuntimeException ex) {
					logger.debug("Ignoring failure to complete Sink: {}", sink, ex);
				}
			}
		}

		/**
		 * If the task has not started, prevent it from ever running and
		 * complete it here so listeners are completed.
		 */
		public void cancelIfNotStarted() {
			if (claimStart()) {
				finish(new CancellationException("MultiSink cancelled before start"));
			}
		}

		public void awaitCompletion() throws InterruptedException {
			this.completionLatch.await();
		}

	}


	/**
	 * Registry to manage a dynamic list of {@code Sink} listeners through one
	 * list for current registrations and two lists for staged additions and
	 * removals.
	 *
	 * <p>Staged updates are applied only by the fanout thread prior to each
	 * dispatch while additions and removals may be staged at any tie by other
	 * threads via top-level, public methods on {@link MultiSink}.
	 */
	private static final class ListenerRegistry<T> {

		/** Written only by the fanout task thread, which applies the changes staged below. */
		private final List<Sink<T>> listeners;

		private final List<Sink<T>> addListeners = new CopyOnWriteArrayList<>();

		private final List<Sink<T>> removeListeners = new CopyOnWriteArrayList<>();

		ListenerRegistry(List<Sink<T>> listeners) {
			this.listeners = new ArrayList<>(listeners);
		}

		public void add(Sink<T> listener) {
			this.addListeners.add(listener);
		}

		public void remove(Sink<T> listener) {
			this.removeListeners.add(listener);
		}

		/**
		 * Return the currently registered listeners. For use only from the fanout
		 * task thread, and mutable, so a failed listener may be removed.
		 */
		List<Sink<T>> listeners() {
			return this.listeners;
		}

		/**
		 * Apply staged additions and removals to the list of listeners.
		 * For use by the fanout thread only.
		 * @return true if there are remaining listeners after the changes
		 */
		boolean applyChanges() {
			// Snapshot removals first to guarantee each removal has a matching addition
			Iterator<Sink<T>> removeIterator = this.removeListeners.iterator();

			while (!this.addListeners.isEmpty()) {
				Sink<T> listener = this.addListeners.remove(0);
				this.listeners.add(listener);
			}

			while (removeIterator.hasNext()) {
				Sink<T> listener = removeIterator.next();
				this.removeListeners.remove(0);
				if (this.listeners.remove(listener)) {
					listener.complete();
				}
			}

			return !this.listeners.isEmpty();
		}

		/**
		 * Complete any listener staged for addition.
		 * For use after the fanout task has ended, from both
		 * {@link MultiSink#registerListener} the fanout task to ensure that
		 * all listeners added late are completed.
		 */
		public void completeStagedAdditions() {
			for (Sink<T> listener : this.addListeners) {
				if (this.addListeners.remove(listener)) {
					listener.complete();
				}
			}
		}
	}

}
