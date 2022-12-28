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

package org.opengauss.datachecker.check.modules.check;

import com.alibaba.fastjson.annotation.JSONType;
import lombok.Data;
import org.opengauss.datachecker.common.constant.Constants.InitialCapacity;
import org.opengauss.datachecker.common.entry.enums.CheckMode;
import org.opengauss.datachecker.common.entry.enums.Endpoint;
import org.springframework.util.CollectionUtils;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * CheckDiffResult
 *
 * @author ：wangchao
 * @date ：Created in 2022/6/18
 * @since ：11
 */
@Data
@JSONType(
    orders = {"schema", "table", "topic", "partitions", "beginOffset", "checkMode", "result", "message", "createTime",
        "keyInsertSet", "keyUpdateSet", "keyDeleteSet", "repairInsert", "repairUpdate", "repairDelete"},
    ignores = {"totalRepair", "buildRepairDml", "isBuildRepairDml"})
public class CheckDiffResult {
    public static final String FAILED_RESULT = "failed";
    private String process;
    private String schema;
    private String table;
    private String topic;
    private int partitions;
    private long beginOffset;
    private int totalRepair;
    private CheckMode checkMode;
    private LocalDateTime createTime;
    private String result;
    private String message;
    private Set<String> keyInsertSet;
    private Set<String> keyUpdateSet;
    private Set<String> keyDeleteSet;
    private List<String> repairInsert;
    private List<String> repairUpdate;
    private List<String> repairDelete;

    /**
     * constructor
     */
    public CheckDiffResult() {
    }

    /**
     * constructor
     *
     * @param builder builder
     */
    public CheckDiffResult(final AbstractCheckDiffResultBuilder<?, ?> builder) {
        table = Objects.isNull(builder.getTable()) ? "" : builder.getTable();
        partitions = builder.getPartitions();
        beginOffset = builder.getBeginOffset();
        topic = Objects.isNull(builder.getTopic()) ? "" : builder.getTopic();
        schema = Objects.isNull(builder.getSchema()) ? "" : builder.getSchema();
        process = Objects.isNull(builder.getProcess()) ? "" : builder.getProcess();
        createTime = builder.getCreateTime();
        checkMode = builder.getCheckMode();
        if (builder.isExistTableMiss()) {
            initEmptyCollections();
            resultTableNotExist(builder.getOnlyExistEndpoint());
        } else if (builder.isTableStructureEquals()) {
            keyUpdateSet = builder.getKeyUpdateSet();
            keyInsertSet = builder.getKeyInsertSet();
            keyDeleteSet = builder.getKeyDeleteSet();
            if (builder.isNotLargeDiffKeys()) {
                repairUpdate = builder.getRepairUpdate();
                repairInsert = builder.getRepairInsert();
                repairDelete = builder.getRepairDelete();
            }
            resultAnalysis(builder.isNotLargeDiffKeys());
        } else {
            initEmptyCollections();
            resultTableStructureNotEquals();
        }
    }

    private void initEmptyCollections() {
        keyUpdateSet = new HashSet<>(InitialCapacity.EMPTY);
        keyInsertSet = new HashSet<>(InitialCapacity.EMPTY);
        keyDeleteSet = new HashSet<>(InitialCapacity.EMPTY);
        repairUpdate = new ArrayList<>(InitialCapacity.EMPTY);
        repairInsert = new ArrayList<>(InitialCapacity.EMPTY);
        repairDelete = new ArrayList<>(InitialCapacity.EMPTY);
    }

    private void resultTableStructureNotEquals() {
        result = FAILED_RESULT;
        message = "table structure is not equals , please check the database sync !";
    }

    private void resultTableNotExist(Endpoint onlyExistEndpoint) {
        result = FAILED_RESULT;
        message = "table [" + table + "] , " + " only exist in " + onlyExistEndpoint.getDescription() + "!";
    }

    private void resultAnalysis(boolean isNotLargeDiffKeys) {
        message = schema + "." + table + "_[" + partitions + "] check ";
        if (CollectionUtils.isEmpty(keyInsertSet) && CollectionUtils.isEmpty(keyUpdateSet) && CollectionUtils
            .isEmpty(keyDeleteSet)) {
            result = "success";
            message += result;
        } else {
            result = FAILED_RESULT;
            message += result;
            message +=
                "( insert=" + keyInsertSet.size() + " update=" + keyUpdateSet.size() + " delete=" + keyDeleteSet.size()
                    + " )";
            if (totalRepair > 0 && !isNotLargeDiffKeys) {
                message += " data error is too large , please check the database sync !";
            }
        }
    }
}
