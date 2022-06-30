package org.opengauss.datachecker.common.entry.check;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import org.springframework.lang.NonNull;

/**
 * @author ：wangchao
 * @date ：Created in 2022/6/1
 * @since ：11
 */
@Getter
@EqualsAndHashCode
public final class Pair<S, T> {

    private S source;
    private T sink;

    private Pair(S source, T sink) {
        this.source = source;
        this.sink = sink;
    }

    /**
     * Creates a new {@link Pair} for the given elements.
     *
     * @param source must not be {@literal null}.
     * @param sink   must not be {@literal null}.
     * @return
     */
    public static <S, T> Pair<S, T> of(S source, T sink) {
        return new Pair<>(source, sink);
    }

    /**
     * 修改pair
     *
     * @param pair
     * @param sink
     * @param <S>
     * @param <T>
     * @return
     */
    public static <S, T> Pair<S, T> of(@NonNull Pair<S, T> pair, T sink) {
        pair.sink = sink;
        return pair;
    }

    public static <S, T> Pair<S, T> of(S source, @NonNull Pair<S, T> pair) {
        pair.source = source;
        return pair;
    }

    /**
     * @return source->sink
     */
    @Override
    public String toString() {
        return String.format("%s->%s", this.source, this.sink);
    }
}
