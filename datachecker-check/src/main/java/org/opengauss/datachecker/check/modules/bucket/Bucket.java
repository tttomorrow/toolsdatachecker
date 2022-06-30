package org.opengauss.datachecker.check.modules.bucket;

import lombok.Data;
import lombok.experimental.Accessors;
import org.opengauss.datachecker.common.entry.extract.RowDataHash;
import org.opengauss.datachecker.common.util.ByteUtil;

import javax.validation.constraints.NotNull;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

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
     * 桶初始化容量，如果桶内数据的数量超过指定数量{@code initialCapacity*0.75} 则会自动触发桶容量扩容。
     */
    private int initialCapacity;

    /**
     * Bucket桶的容器,容器的初始化容量大小为设置为平均容量大小。
     * <p>
     * 超出平均容量会进行扩容操作
     */
    private Map<String, RowDataHash> bucket = new HashMap<>(this.initialCapacity);
    /**
     * 桶编号
     */
    private Integer number;
    /**
     * Bucket桶的哈希签名 ,签名初始化值为0
     */
    private long signature = 0L;

    /**
     * 桶构造时，要求进行容量大小初始化
     *
     * @param initialCapacity 桶初始化容量大小
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
