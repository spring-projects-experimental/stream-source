package io.spring.streamsource.flow;

import java.io.IOException;
import java.util.Objects;
import java.util.concurrent.Executor;
import java.util.concurrent.Flow.Publisher;
import java.util.concurrent.Flow.Subscriber;
import java.util.concurrent.Flow.Subscription;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.LockSupport;

import io.spring.streamsource.Source;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@link Publisher} that receives from a pull-style {@link Source} on an
 * {@link Executor}, turning the blocking receive loop into Reactive Streams signals.
 * Created through {@link FlowSourceAdapter#toPublisher(Source)}.
 *
 * <p>The loop thread is where the blocking happens, and demand is what it waits for:
 * with no outstanding {@code request(n)} it parks rather than calling
 * {@link Source#receiveNext()}, so nothing is received that nobody asked for. Because
 * the loop parks, the executor must be one where blocking is allowed.
 *
 * <p>Waiting for demand also means the end of the stream is not discovered until it is
 * asked for: {@code onComplete} is signalled by the receive that follows the last item,
 * which requires demand like any other.
 *
 * <p>A Source has a single consumer, so this Publisher accepts a single subscriber.
 *
 * @param <T> the type of items received through the Source
 */
final class SourcePublisher<T> implements Publisher<T> {

	private final Source<T> source;

	private final Executor executor;

	private final AtomicBoolean subscribed = new AtomicBoolean();


	SourcePublisher(Source<T> source, Executor executor) {
		this.source = source;
		this.executor = executor;
	}


	@Override
	public void subscribe(Subscriber<? super T> subscriber) {
		Objects.requireNonNull(subscriber, "Subscriber must not be null");
		if (!this.subscribed.compareAndSet(false, true)) {
			subscriber.onSubscribe(NoOpSubscription.INSTANCE);
			subscriber.onError(new IllegalStateException(
					"A Source has a single consumer, so " + this + " allows a single subscriber"));
			return;
		}
		ReceiveLoop<T> loop = new ReceiveLoop<>(this.source, subscriber);
		subscriber.onSubscribe(loop);
		if (loop.isCancelled()) {
			// Cancelled from onSubscribe: no loop thread will ever run, so nothing
			// will reach the finally in run() that closes the Source; hand that off
			// to the executor instead of closing here, on what may be an NIO thread.
			loop.signalPendingError();
			try {
				this.executor.execute(this.source::close);
			}
			catch (RejectedExecutionException ex) {
				this.source.close();
			}
			return;
		}
		try {
			this.executor.execute(loop);
		}
		catch (RejectedExecutionException ex) {
			loop.rejected(ex);
		}
	}

	@Override
	public String toString() {
		return getClass().getSimpleName() + " for " + this.source;
	}


	/**
	 * The receive loop, and the {@link Subscription} handed to the subscriber. Items are
	 * emitted from the loop thread only, which is what serializes the signals.
	 */
	private static final class ReceiveLoop<T> implements Subscription, Runnable {

		private static final Logger logger = LoggerFactory.getLogger(SourcePublisher.class);

		private static final int IDLE = 0;

		private static final int RECEIVING = 1;

		private static final int INTERRUPTING = 2;

		private static final int INTERRUPTED = 3;

		private final Source<T> source;

		private final Subscriber<? super T> actual;

		/** Outstanding demand, {@code Long.MAX_VALUE} meaning unbounded. */
		private final AtomicLong requested = new AtomicLong();

		/** Error from an invalid {@code request(n)}, signalled by the loop thread. */
		private final AtomicReference<@Nullable Throwable> pendingError = new AtomicReference<>();

		/** Set by {@link #run()} itself, so the signal side has something to wake or interrupt. */
		private volatile @Nullable Thread loopThread;

		/** Whether the loop is inside {@link Source#receiveNext()}, the only place {@link #cancel()} interrupts. */
		private final AtomicInteger phase = new AtomicInteger(IDLE);

		private volatile boolean cancelled;


		ReceiveLoop(Source<T> source, Subscriber<? super T> actual) {
			this.source = source;
			this.actual = actual;
		}


		// Subscriber side, called from any thread

		@Override
		public void request(long n) {
			if (n <= 0) {
				// Cancel, and let the loop thread signal the error, so that it is not
				// concurrent with an onNext in progress.
				if (!this.cancelled) {
					this.pendingError.compareAndSet(null, new IllegalArgumentException(
							"request amount must be positive, but it was " + n));
					cancel();
				}
				return;
			}
			this.requested.getAndUpdate(r -> {
				long sum = r + n;
				return (sum < 0 ? Long.MAX_VALUE : sum);
			});
			unpark();
		}

		/**
		 * {@inheritDoc}
		 * <p>Never blocks the calling thread. A receive in progress is interrupted, and
		 * the loop thread then closes the Source.
		 */
		@Override
		public void cancel() {
			if (this.cancelled) {
				return;
			}
			this.cancelled = true;
			// Closing here would block the caller, which may be an event loop, and use the
			// Source from a second thread; the loop thread closes it in run() instead
			Thread thread = this.loopThread;
			if (thread == null) {
				return;
			}
			// Only a receive needs interrupting: a parked awaitDemand() needs an unpark,
			// and the subscriber's own code is followed by a check of cancelled anyway
			if (this.phase.compareAndSet(RECEIVING, INTERRUPTING)) {
				try {
					thread.interrupt();
				}
				finally {
					this.phase.set(INTERRUPTED);
				}
			}
			else if (thread != Thread.currentThread()) {
				LockSupport.unpark(thread);
			}
		}

		private void unpark() {
			Thread thread = this.loopThread;
			if (thread != null) {
				LockSupport.unpark(thread);
			}
		}

		boolean isCancelled() {
			return this.cancelled;
		}

		void rejected(RejectedExecutionException ex) {
			this.source.close();
			if (!this.cancelled) {
				this.actual.onError(ex);
			}
			else {
				signalPendingError();
			}
		}

		void signalPendingError() {
			Throwable ex = this.pendingError.getAndSet(null);
			if (ex != null) {
				this.actual.onError(ex);
			}
		}


		// Loop side, on the executor thread

		@Override
		public void run() {
			this.loopThread = Thread.currentThread();
			try {
				while (awaitDemand()) {
					if (!receiveNext()) {
						complete();
						return;
					}
					T item = this.source.next();
					if (this.cancelled) {
						logger.debug("cancelled, discarding {}", item);
						break;
					}
					this.actual.onNext(item);
					produced();
				}
				signalPendingError();
			}
			catch (InterruptedException ex) {
				// An interrupt from cancel() was cleared on the way out of receiveNext()
				if (!this.cancelled) {
					Thread.currentThread().interrupt();
				}
				error(ex);
			}
			catch (Throwable ex) {
				error(ex);
			}
			finally {
				this.source.close();
			}
		}

		private boolean receiveNext() throws IOException, InterruptedException {
			this.phase.set(RECEIVING);
			try {
				// Checked after publishing the phase: a cancel() that saw IDLE did not interrupt
				return (!this.cancelled && this.source.receiveNext());
			}
			finally {
				if (!this.phase.compareAndSet(RECEIVING, IDLE)) {
					// cancel() got there first: wait for its interrupt to land, then clear
					// it, so that it cannot reach the subscriber, close(), or whatever the
					// executor runs next on this thread
					while (this.phase.get() == INTERRUPTING) {
						Thread.yield();
					}
					Thread.interrupted();
					this.phase.set(IDLE);
				}
			}
		}

		/**
		 * Park until there is demand to receive one more item, and report whether the
		 * loop should carry on. A permit made available by {@link #unpark()} before
		 * this parks is not lost, so a {@code request(n)} racing with the check below
		 * still wakes it.
		 */
		private boolean awaitDemand() throws InterruptedException {
			while (!this.cancelled && this.requested.get() == 0) {
				LockSupport.park();
				// Not from cancel(), which only unparks here; an executor shutting down,
				// say, would otherwise make every park() return at once and spin
				if (Thread.interrupted()) {
					throw new InterruptedException();
				}
			}
			return !this.cancelled;
		}

		private void produced() {
			this.requested.getAndUpdate(r -> (r == Long.MAX_VALUE ? r : r - 1));
		}

		private void complete() {
			if (!this.cancelled) {
				this.actual.onComplete();
			}
			else {
				signalPendingError();
			}
		}

		private void error(Throwable ex) {
			if (this.cancelled) {
				if (this.pendingError.get() != null) {
					signalPendingError();
				}
				else {
					// Nobody left to report to, and typically the Source unwinding from
					// the interrupt that cancel() sent, like a ClosedByInterruptException
					// from an interrupted read.
					logger.debug("cancelled, discarding {}", ex.toString());
				}
			}
			else {
				this.actual.onError(ex);
			}
		}
	}


	private static final class NoOpSubscription implements Subscription {

		static final NoOpSubscription INSTANCE = new NoOpSubscription();

		@Override
		public void request(long n) {
		}

		@Override
		public void cancel() {
		}
	}

}
