package org.opengauss.datachecker.common.entry.check;

import lombok.EqualsAndHashCode;
import lombok.Getter;

/**
 * @author ：wangchao
 * @date ：Created in 2022/6/1
 * @since ：11
 */
@Getter
@EqualsAndHashCode
public final class DifferencePair<L, R, D> {

    private final L onlyOnLeft;
    private final R onlyOnRight;
    private final D differing;

    private DifferencePair(L onlyOnLeft, R onlyOnRight, D differing) {
        this.onlyOnLeft = onlyOnLeft;
        this.onlyOnRight = onlyOnRight;
        this.differing = differing;
    }

    /**
     * Creates a new {@link DifferencePair} for the given elements.
     *
     * @param onlyOnLeft  must not be {@literal null}.
     * @param onlyOnRight must not be {@literal null}.
     * @param differing   must not be {@literal null}.
     * @return
     */
    public static <L, R, D> DifferencePair<L, R, D> of(L onlyOnLeft, R onlyOnRight, D differing) {
        return new DifferencePair<>(onlyOnLeft, onlyOnRight, differing);
    }

    /**
     * @return differing : onlyOnLeft -> onlyOnRight
     */
    @Override
    public String toString() {
        return String.format("%s : %s->%s", this.differing, this.onlyOnLeft, this.onlyOnRight);
    }
}
