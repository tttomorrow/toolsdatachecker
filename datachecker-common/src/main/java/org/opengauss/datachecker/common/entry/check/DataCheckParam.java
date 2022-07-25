package org.opengauss.datachecker.common.entry.check;

import lombok.Getter;
import org.opengauss.datachecker.common.entry.extract.Topic;
import org.springframework.lang.NonNull;

/**
 * 数据校验线程参数
 *
 * @author ：wangchao
 * @date ：Created in 2022/6/10
 * @since ：11
 */
@Getter
public class DataCheckParam {

    /**
     * 构建桶容量参数
     */
    private final int bucketCapacity;

    /**
     * 数据校验TOPIC对象
     */
    private final Topic topic;
    /**
     * 校验Topic 分区
     */
    private final int partitions;
    /**
     * 校验结果输出路径
     */
    private final String path;

    private final String schema;

    /**
     * 校验参数构建器
     *
     * @param bucketCapacity 构建桶容量参数
     * @param topic          数据校验TOPIC对象
     * @param partitions     校验Topic 分区
     * @param path           校验结果输出路径
     */
    public DataCheckParam(int bucketCapacity, @NonNull Topic topic, int partitions, @NonNull String path, String schema) {
        this.bucketCapacity = bucketCapacity;
        this.topic = topic;
        this.partitions = partitions;
        this.path = path;
        this.schema = schema;
    }
}
