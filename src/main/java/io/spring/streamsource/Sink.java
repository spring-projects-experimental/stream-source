package io.spring.streamsource;

import java.io.IOException;

/**
 * Contract to send the items of a stream.
 *
 * <p>A producer sends any number of items with {@link #send}, and then completes
 * the Sink via {@link #complete()} or {@link #completeExceptionally} to end with
 * success or an error. After a Sink is complete, or can otherwise no longer
 * send, a call to {@link #send} throws {@link SinkCompleteException}.
 * Producers must always complete the Sink to ensure consumers don't wait
 * indefinitely.
 *
 * <pre class="code">
 * try {
 *     sink.send("one");
 *     sink.send("two");
 *     sink.complete();
 * }
 * catch (InterruptedException ex) {
 *     Thread.currentThread().interrupt();
 *     sink.completeExceptionally(new RuntimeException("Sending interrupted", ex));
 * }
 * catch (Throwable ex) {
 *     sink.completeExceptionally(ex);
 * }
 * </pre>
 *
 * <p>Callers should not assume that a Sink supports concurrent use unless that
 * is explicitly stated as supported by the implementation. That means
 * implementations can assume serial calls, but may also choose to support
 * concurrent calls.
 *
 * <p>Sink is typically backed by {@link Exchange}, which enables a hand-off
 * between an independent producer and consumer.
 * {@link io.spring.streamsource.task.Producer} is a contract for a producer
 * task that can be launched as a background task through
 * {@link io.spring.streamsource.task.ActiveSource}.
 *
 * @param <T> the type of items sent to the Sink
 * @see Exchange
 * @see SinkCompleteException
 * @see io.spring.streamsource.task.Producer
 */
public interface Sink<T> {

	/**
	 * Send the next item, blocking if necessary until the Sink can accept it.
	 * Blocking is how the Sink applies backpressure, so the call may block
	 * for as long as the consumer cannot accept the item.
	 * <p>If this method throws, the item was not sent.
	 * @param item the item to send, never {@code null}
	 * @throws SinkCompleteException if the Sink can no longer accept items,
	 * because it was completed or because the consumer closed the
	 * {@code Source} side of an {@link Exchange}. A producer should treat
	 * this as the signal to stop sending.
	 * @throws IOException if sending encounters an I/O error
	 * @throws InterruptedException if send was interrupted, without the item
	 * being sent; the Sink is not completed, which is up to the producer, for
	 * example with {@link #completeExceptionally}, see
	 * {@link io.spring.streamsource.task.Producer#produceAndComplete(Sink)}
	 */
	void send(T item) throws IOException, InterruptedException;

	/**
	 * Complete the Sink normally, signaling there are no more items to send.
	 * <p>This method must not block, even if the Sink is full, and does not
	 * throw {@link SinkCompleteException}. It is idempotent: if the Sink is
	 * already completed, normally or with an error, the call has no effect and
	 * the first completion wins.
	 */
	void complete();

	/**
	 * Complete the Sink with an error, signaling no more items will be sent.
	 * <p>This method must not block, even if the Sink is full, and does not
	 * throw {@link SinkCompleteException}. It is idempotent: if the Sink is
	 * already completed, normally or with an error, the call has no effect and
	 * the first completion wins.
	 * @param cause the error that ended the stream, never {@code null}
	 */
	void completeExceptionally(Throwable cause);

}
