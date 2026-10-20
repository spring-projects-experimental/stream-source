package io.spring.streamsource.flow;

import reactor.blockhound.BlockHound;
import reactor.blockhound.BlockingOperationError;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class BlockHoundSmokeTest {

	static {
		BlockHound.install();
	}

	@Test
	void sleepOnParallelSchedulerIsDetected() {
		assertThatThrownBy(() ->
				Mono.fromRunnable(() -> {
					try {
						Thread.sleep(10);
					}
					catch (InterruptedException e) {
						throw new RuntimeException(e);
					}
				})
				.subscribeOn(Schedulers.parallel())
				.block())
				.hasCauseInstanceOf(BlockingOperationError.class);
	}
}
