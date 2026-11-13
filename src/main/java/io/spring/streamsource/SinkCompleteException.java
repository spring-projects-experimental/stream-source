package io.spring.streamsource;

import java.io.Serial;

import org.jspecify.annotations.Nullable;

/**
 * Thrown by {@link Sink#send} when the Sink can no longer accept items because
 * the Sink was completed, whether normally or with an error, or because an
 * {@link Exchange} was closed from the {@code Source} side, and there is no
 * one to consume the items. If the Sink was completed with an error, that
 * error is available as the {@linkplain #getCause() cause}.
 */
public class SinkCompleteException extends RuntimeException {

	@Serial
	private static final long serialVersionUID = -4503267191314132852L;


	/**
	 * Create an instance with the given message.
	 * @param message the detail message
	 */
	public SinkCompleteException(@Nullable String message) {
		super(message);
	}

	/**
	 * Create an instance with the given message and cause.
	 * @param message the detail message
	 * @param cause the error the Sink was completed with, if any
	 */
	public SinkCompleteException(@Nullable String message, @Nullable Throwable cause) {
		super(message, cause);
	}

}
