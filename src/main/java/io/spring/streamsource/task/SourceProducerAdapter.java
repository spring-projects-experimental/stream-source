package io.spring.streamsource.task;

import io.spring.streamsource.Sink;
import io.spring.streamsource.Source;

/**
 * {@link Producer} that produces items consumed from a {@link Source}.
 * @param <T> the type of items produced
 */
final class SourceProducerAdapter<T> implements Producer<T> {

	private final Source<T> source;


	public SourceProducerAdapter(Source<T> source) {
		this.source = source;
	}


	@Override
	public void produce(Sink<T> sink) throws Throwable {
		try (this.source) {
			while (this.source.receiveNext()) {
				sink.send(this.source.next());
			}
		}
	}

}
