package io.spring.streamsource.task;

import io.spring.streamsource.Source;
import io.spring.streamsource.TestSink;
import io.spring.streamsource.TestSource;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for {@link SourceProducerAdapter}.
 */
public class SourceProducerAdapterTests {


	@Test
	void receiveAndComplete() {
		TestSource<String> source = TestSource.withItems("item 1", "item2");
		TestSink<String> sink = TestSink.create();
		SourceProducerAdapter<String> adapter = new SourceProducerAdapter<>(source);
		adapter.produceAndComplete(sink);

		assertThat(sink.getSentItems()).containsExactly("item 1", "item2");
		assertThat(sink.getCompletionException()).isNull();
	}

	@Test
	void receiveAndCompleteExceptionally() {
		IllegalStateException ex = new IllegalStateException("boom");
		Source<String> source = TestSource.withItems("item 1", "item2").failure(ex);
		TestSink<String> sink = TestSink.create();
		SourceProducerAdapter<String> adapter = new SourceProducerAdapter<>(source);
		adapter.produceAndComplete(sink);

		assertThat(sink.getSentItems()).containsExactly("item 1", "item2");
		assertThat(sink.getCompletionException()).isSameAs(ex);
	}

}
