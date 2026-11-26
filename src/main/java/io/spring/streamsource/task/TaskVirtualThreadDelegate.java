package io.spring.streamsource.task;

import java.util.concurrent.ExecutorService;

/**
 * Internal delegate for virtual thread handling on JDK 21.
 * This is a dummy version for reachability on JDK <21.
 *
 * @see SourceTask
 */
final class TaskVirtualThreadDelegate {

	public TaskVirtualThreadDelegate() {
		throw new UnsupportedOperationException("Virtual threads not supported on JDK <21");
	}

	public ExecutorService newVirtualThreadPerTaskExecutor() {
		throw new UnsupportedOperationException();
	}

}
