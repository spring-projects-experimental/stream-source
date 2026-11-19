package io.spring.streamsource.task;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;

import io.spring.streamsource.BlockingQueueExchange;
import io.spring.streamsource.Exchange;
import io.spring.streamsource.Source;
import org.jspecify.annotations.Nullable;


/**
 * A helper class to start a {@link Producer} that pushes items into an
 * {@link Exchange} on a background thread. A consumer can then receive from the
 * {@link Source} side of the exchange, independent of the producer, and with
 * backpressure provided by the exchange.
 *
 * <p>Use the available static factory methods to create an {@code ActiveSource},
 * and then call {@link #start()} to start the {@code Producer} and obtain the
 * {@code Source} side of the exchange to receive from.
 *
 * <pre class="code">
 * ActiveSource&lt;String&gt; activeSource = ActiveSource.fromProducer(producer);
 *
 * try (Source&lt;String&gt; source = activeSource.start()) {
 *     while (source.receiveNext()) {
 *         String item = source.next();
 *         // process item
 *     }
 * }
 * </pre>
 *
 * <p>Closing the Source returned from {@code start()} cancels the producer by
 * interrupting its thread, and then waits for the task to finish, blocking for
 * as long as the producer takes to respond to the interrupt, see
 * <a href="https://github.com/spring-projects-experimental/stream-source/blob/main/CANCELLATION.md">CANCELLATION.md</a>.
 * The consumer must close the {@code Source}, e.g. via try-with-resources,
 * or otherwise once the exchange is full, the producer will block in
 * {@code send} and hold on to its thread.
 *
 * <p>By default, the {@code Producer} runs on a virtual thread, which requires
 * Java 21+. On Java &lt; 21, use the factory methods accepting an
 * {@link ExecutorService}.
 *
 * @param <T> the type of items produced
 */
public final class ActiveSource<T> {

	private static final @Nullable ExecutorService defaultExecutorService = createDefaultExecutorService();


	private final Producer<T> producer;

	private final ExecutorService executorService;

	private @Nullable Integer capacity;


	private ActiveSource(Producer<T> producer, ExecutorService executorService) {
		this.producer = producer;
		this.executorService = executorService;
	}


	/**
	 * Set the capacity for the internal {@link Exchange} that the
	 * {@code Producer} pushes into and that backs the active {@code Source}.
	 * <p>The capacity is the number of items the {@code Exchange} holds before
	 * the producer blocks, and must be greater than zero.
	 * <p>By default, it is {@value BlockingQueueExchange#DEFAULT_CAPACITY}.
	 * @param capacity the capacity to use, in number of items
	 * @return the same instance for chaining
	 */
	public ActiveSource<T> capacity(int capacity) {
		this.capacity = capacity;
		return this;
	}


	/**
	 * Submit the Producer to run on a background thread so it can begin pushing
	 * items into the Exchange while the Source can be read independently.
	 * <p>This method returns immediately after submitting the task.
	 * @return the {@code Source} side of the Exchange to receive from, which
	 * stops the producer when closed
	 */
	public Source<T> start() {
		Exchange<T> exchange = initExchange();
		ProducerTask<T> task = new ProducerTask<>(this.producer, exchange);
		Future<?> future = this.executorService.submit(task);
		return new ProducingExchange<>(exchange, future, task);
	}

	private BlockingQueueExchange<T> initExchange() {
		return (this.capacity != null ?
				new BlockingQueueExchange<>(this.capacity) : new BlockingQueueExchange<>());
	}


	/**
	 * Create an instance that produces items consumed from another {@code Source},
	 * running on a virtual thread.
	 * <p>Requires Java 21+, use {@link #fromSource(Source, ExecutorService)} on
	 * Java &lt; 21.
	 * @param <T> the type of items produced
	 * @param source the source to consume from
	 * @return the created instance
	 * @throws IllegalStateException if virtual threads are not supported
	 */
	public static <T> ActiveSource<T> fromSource(Source<T> source) {
		return fromProducer(Producer.fromSource(source));
	}

	/**
	 * Create an instance that produces items consumed from another {@code Source},
	 * running on the given {@code ExecutorService}.
	 * <br>
	 * <em>Ensure the consumer of the created instance and the reading of the given
	 * {@code Source} run on different {@code ExecutorService} instances to avoid
	 * deadlocks.</em>
	 * @param <T> the type of items produced
	 * @param source the source to consume from
	 * @param executorService the executor service used to run the {@code Producer};
	 * can be used to provide a non-virtual threads {@code ExecutorService} (e.g. a
	 * thread pool with platform threads), compatible with Java &lt; 21. The caller
	 * is responsible for managing its lifecycle.
	 * @return the created instance
	 */
	public static <T> ActiveSource<T> fromSource(Source<T> source, ExecutorService executorService) {
		return fromProducer(Producer.fromSource(source), executorService);
	}

	/**
	 * Create an instance with the given {@code Producer}, running on a virtual thread.
	 * <p>Requires Java 21+, use {@link #fromProducer(Producer, ExecutorService)} on
	 * Java &lt; 21.
	 * @param <T> the type of items produced
	 * @param producer the producer of items
	 * @return the created instance
	 * @throws IllegalStateException if virtual threads are not supported
	 */
	public static <T> ActiveSource<T> fromProducer(Producer<T> producer) {
		ExecutorService executorService = defaultExecutorService;
		if (executorService == null) {
			throw new IllegalStateException(
					"Virtual threads require Java 21+, " +
							"use alternative factory methods that accept an ExecutorService");
		}
		return fromProducer(producer, executorService);
	}

	/**
	 * Create an instance with the given {@code Producer}, running on the given
	 * {@code ExecutorService}.
	 * <br>
	 * <em>Ensure the consumer and producer run on different {@code ExecutorService}
	 * instances to avoid deadlocks.</em>
	 * @param <T> the type of items produced
	 * @param producer the producer of items
	 * @param executorService the executor service used to run the {@code Producer};
	 * can be used to provide a non-virtual threads {@code ExecutorService} (e.g. a
	 * thread pool with platform threads), compatible with Java &lt; 21. The caller
	 * is responsible for managing its lifecycle.
	 * @return the created instance
	 */
	public static <T> ActiveSource<T> fromProducer(Producer<T> producer, ExecutorService executorService) {
		return new ActiveSource<>(producer, executorService);
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
	 * {@link Source} that delegates to the producer-backing {@link Exchange},
	 * and stops the producer task if the {@code Source} is closed.
	 */
	private static final class ProducingExchange<T> implements Source<T> {

		private final Exchange<T> exchange;

		private final Future<?> future;

		private final ProducerTask<T> task;

		private ProducingExchange(Exchange<T> exchange, Future<?> future, ProducerTask<T> task) {
			this.exchange = exchange;
			this.future = future;
			this.task = task;
		}

		@Override
		public boolean receiveNext() throws IOException, InterruptedException {
			return this.exchange.receiveNext();
		}

		@Override
		public T next() {
			return this.exchange.next();
		}

		@Override
		public void close() {
			cancelProducer();
			this.exchange.close();
		}

		private void cancelProducer() {
			this.future.cancel(true);
			this.task.awaitCompletion();
		}
	}


	/**
	 * Runnable wrapper that runs the Producer to help track actual completion
	 * when the task is canceled during execution, or to skip execution when
	 * canceled before it starts.
	 */
	private static final class ProducerTask<T> implements Runnable {

		private final Producer<T> producer;

		private final Exchange<T> exchange;

		private final CountDownLatch completionLatch = new CountDownLatch(1);

		private final AtomicBoolean running = new AtomicBoolean();

		private ProducerTask(Producer<T> producer, Exchange<T> exchange) {
			this.producer = producer;
			this.exchange = exchange;
		}

		@Override
		public void run() {
			if (claimStart()) {
				try {
					this.producer.produceAndComplete(this.exchange);
				}
				finally {
					this.completionLatch.countDown();
				}
			}
		}

		private boolean claimStart() {
			return this.running.compareAndSet(false, true);
		}

		public void awaitCompletion() {
			if (!claimStart()) {
				try {
					this.completionLatch.await();
				}
				catch (InterruptedException ex) {
					Thread.currentThread().interrupt();
				}
			}
		}
	}

}
