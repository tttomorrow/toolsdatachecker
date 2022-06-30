package org.opengauss.datachecker.check.config;

import com.alibaba.fastjson.annotation.JSONType;
import lombok.Data;
import org.hibernate.validator.constraints.Range;
import org.opengauss.datachecker.common.entry.enums.CheckBlackWhiteMode;
import org.opengauss.datachecker.common.exception.CheckingAddressConflictException;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;
import org.springframework.validation.annotation.Validated;

import javax.annotation.PostConstruct;
import javax.validation.constraints.NotEmpty;
import java.util.Objects;

/**
 * @author ：wangchao
 * @date ：Created in 2022/5/23
 * @since ：11
 */
@Data
@Validated
@Component
@ConfigurationProperties(prefix = "data.check")
@JSONType(orders = {"sourceUri", "sinkUri", "bucketExpectCapacity", "healthCheckApi", "dataPath", "blackWhiteMode"})
public class DataCheckProperties {

    @PostConstruct
    private void checkUrl() {
        if (Objects.equals(sourceUri, sinkUri)) {
            // 源端和宿端的访问地址冲突，请重新配置。
            throw new CheckingAddressConflictException("The access addresses of the source end and the destination end conflict, please reconfigure.");
        }
    }


    /**
     * 数据校验服务地址：源端 源端地址不能为空
     */
    @NotEmpty(message = "Source address cannot be empty")
    private String sourceUri;

    /**
     * 数据校验服务地址：宿端 宿端地址不能为空")
     */
    @NotEmpty(message = "The destination address cannot be empty")
    private String sinkUri;

    /**
     * 桶容量 默认容量大小为 1
     */
    @Range(min = 1, message = "The minimum barrel capacity is 1")
    private int bucketExpectCapacity = 1;

    /**
     * 健康检查地址
     */
    private String healthCheckApi;
    /**
     * 数据结果根目录,数据校验结果根目录不能为空
     */
    @NotEmpty(message = "The root directory of data verification results cannot be empty")
    private String dataPath;

    /**
     * 是否增加黑白名单配置
     */
    private CheckBlackWhiteMode blackWhiteMode;
}
