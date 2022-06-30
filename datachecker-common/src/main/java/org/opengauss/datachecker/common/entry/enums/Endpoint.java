package org.opengauss.datachecker.common.entry.enums;

import io.swagger.v3.oas.annotations.media.Schema;
import lombok.Getter;

/**
 * {@value API_DESCRIPTION}
 *
 * @author ：wangchao
 * @date ：Created in 2022/5/25
 * @since ：11
 */
@Schema(description = "data verification endpoint type")
@Getter
public enum Endpoint {
    /**
     * 源端
     */
    SOURCE(1, "SourceEndpoint"),
    /**
     * 宿端
     */
    SINK(2, "SinkEndpoint"),
    /**
     * 校验端
     */
    CHECK(3, "CheckEndpoint");

    private final int code;
    private final String description;

    Endpoint(int code, String description) {
        this.code = code;
        this.description = description;
    }

    public static final String API_DESCRIPTION = "data verification endpoint type " +
            "[SOURCE-1-SourceEndpoint,SINK-2-SinkEndpoint,CHECK-3-CheckEndpoint]";
}
