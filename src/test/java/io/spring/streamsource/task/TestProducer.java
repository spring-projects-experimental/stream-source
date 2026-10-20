package io.spring.streamsource.task;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import io.spring.streamsource.Sink;

/**
 * A {@link Producer} for testing that emits a fixed set of items possibly
 * followed by an exception.
 */
public class TestProducer<T> implements Producer<T> {

	private final List<Object> items = new ArrayList<>();

	private Iterator<Object> iterator;


	private TestProducer() {
	}


	@SafeVarargs
	public final TestProducer<T> items(T... items) {
		this.items.addAll(Arrays.asList(items));
		return this;
	}

	public Producer<T> failure(Throwable ex) {
		this.items.add(ex);
		return this;
	}

	public TestProducer<T> produceStep(ProduceStep<T> step) {
		this.items.add(step);
		return this;
	}


	@SafeVarargs
	public static <S> TestProducer<S> withItems(S... items) {
		return new TestProducer<S>().items(items);
	}

	public static <S> Producer<S> withFailure(Throwable ex) {
		return new TestProducer<S>().failure(ex);
	}

	public static <S> TestProducer<S> withProduceStep(ProduceStep<S> step) {
		return new TestProducer<S>().produceStep(step);
	}


	@SuppressWarnings("unchecked")
	@Override
	public void produce(Sink<T> sink) throws Throwable {
		initIterator();
		while (this.iterator.hasNext()) {
			Object next = this.iterator.next();
			if (next instanceof Throwable ex) {
				throw ex;
			}
			else if (next instanceof ProduceStep<?>) {
				ProduceStep<T> step = (ProduceStep<T>) next;
				step.execute(sink);
			}
			else {
				sink.send((T) next);
			}
		}
	}

	private void initIterator() {
		if (this.iterator == null) {
			this.iterator = this.items.iterator();
		}
	}


	@FunctionalInterface
	public interface ProduceStep<T> {

		void execute(Sink<T> sink) throws Throwable;

	}

}
