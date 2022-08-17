/*
 * Copyright (c) 2022-2022 Huawei Technologies Co.,Ltd.
 *
 * openGauss is licensed under Mulan PSL v2.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *
 *           http://license.coscl.org.cn/MulanPSL2
 *
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 */

package org.opengauss.datachecker.check.modules.bucket;

import lombok.Data;
import lombok.experimental.Accessors;
import org.opengauss.datachecker.common.entry.extract.RowDataHash;
import org.opengauss.datachecker.common.util.ByteUtil;

import javax.validation.constraints.NotNull;
import java.io.Serializable;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author ：wangchao
 * @date ：Created in 2022/5/23
 * @since ：11
 */
@Data
@Accessors(chain = true)
public class Bucket implements Serializable {

    private static final long serialVersionUID = 1L;
    /**
     * <pre>
     * Bucket initialization capacity.
     * If the amount of data in the bucket exceeds the specified amount {@code initialCapacity*0.75},
     * the bucket capacity expansion will be automatically triggered.
     * </pre>
     */
    private int initialCapacity;

    /**
     * <pre>
     * The initialization capacity of the bucket container is set to the average capacity.
     * <p>
     * If the average capacity is exceeded, the capacity will be expanded
     * </pre>
     */
    private Map<String, RowDataHash> bucket = new ConcurrentHashMap<>(initialCapacity);
    /**
     * bucket number
     */
    private Integer number;
    /**
     * Hash signature of bucket bucket. The initialization value of the signature is 0
     */
    private long signature = 0L;

    /**
     * Capacity initialization is required during barrel construction
     *
     * @param initialCapacity Bucket initialization capacity size
     */
    public Bucket(int initialCapacity) {
        this.initialCapacity = initialCapacity;
    }

    /**
     * 将行记录哈希对象添加到桶容器中。并计算桶的哈希签名。
     * <p>
     * 桶的哈希签名算法为当前桶的哈希签名{@code signature}异或当前插入记录的行哈希值。
     *
     * @param rowDataHash 行记录哈希对象
     * @return 返回插入集合结果
     */
    public RowDataHash put(@NotNull RowDataHash rowDataHash) {
        signature = signature ^ rowDataHash.getRowHash();
        return bucket.put(rowDataHash.getPrimaryKey(), rowDataHash);
    }

    /**
     * 获取当前桶的哈希签名
     *
     * @return 桶的哈希签名
     */
    public byte[] getSignature() {
        return ByteUtil.toBytes(signature);
    }
}
