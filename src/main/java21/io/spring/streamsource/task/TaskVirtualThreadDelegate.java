package io.spring.streamsource.task;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Internal delegate for virtual thread handling on JDK 21+.
 * This is the actual version compiled against JDK 21.
 *
 * @see SourceTask
 */
final class TaskVirtualThreadDelegate {

	public TaskVirtualThreadDelegate() {
	}

	public ExecutorService newVirtualThreadPerTaskExecutor() {
		return Executors.newVirtualThreadPerTaskExecutor();
	}

}
