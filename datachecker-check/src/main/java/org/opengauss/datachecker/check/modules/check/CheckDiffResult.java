package org.opengauss.datachecker.check.modules.check;

import com.alibaba.fastjson.annotation.JSONType;
import lombok.Data;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Set;

/**
 * @author ：wangchao
 * @date ：Created in 2022/6/18
 * @since ：11
 */
@Data
@JSONType(orders = {"schema", "table", "topic", "partitions", "createTime",
        "keyInsertSet", "keyUpdateSet", "keyDeleteSet",
        "repairInsert", "repairUpdate", "repairDelete"})
public class CheckDiffResult {
    private String schema;
    private String table;
    private String topic;
    private int partitions;
    private LocalDateTime createTime;

    private Set<String> keyInsertSet;
    private Set<String> keyUpdateSet;
    private Set<String> keyDeleteSet;

    private List<String> repairInsert;
    private List<String> repairUpdate;
    private List<String> repairDelete;

    public CheckDiffResult(final AbstractCheckDiffResultBuilder<?, ?> builder) {
        this.table = builder.getTable();
        this.partitions = builder.getPartitions();
        this.topic = builder.getTopic();
        this.schema = builder.getSchema();
        this.createTime = builder.getCreateTime();
        this.keyUpdateSet = builder.getKeyUpdateSet();
        this.keyInsertSet = builder.getKeyInsertSet();
        this.keyDeleteSet = builder.getKeyDeleteSet();
        this.repairUpdate = builder.getRepairUpdate();
        this.repairInsert = builder.getRepairInsert();
        this.repairDelete = builder.getRepairDelete();
    }
}
