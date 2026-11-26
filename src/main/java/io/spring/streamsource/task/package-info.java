/**
 * Task layer over the core abstractions to stream data, including:
 * <ul>
 * <li>{@link io.spring.streamsource.task.SourceTask}, which runs a
 * {@link io.spring.streamsource.task.Producer} on a background thread
 * to decouple production from consumption.
 * <li>{@link io.spring.streamsource.task.MultiSinkTask}, which runs a backround
 * task to fan out items to a list of {@link io.spring.streamsource.Sink}
 * listeners.
 * </ul>
 *
 * <p>The package is null-marked: types are non-null unless annotated
 * {@link org.jspecify.annotations.Nullable}. That includes the item type of a Source,
 * which the implementation relies on, since it uses {@code null} internally to mean
 * "no item"; a {@code Source<@Nullable String>} is therefore not supported.
 */
@NullMarked
package io.spring.streamsource.task;

import org.jspecify.annotations.NullMarked;
