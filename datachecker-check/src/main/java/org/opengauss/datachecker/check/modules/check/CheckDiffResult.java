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
@JSONType(orders = {"schema", "table", "topic", "partitions", "result", "message", "createTime", "keyInsertSet",
    "keyUpdateSet", "keyDeleteSet", "repairInsert", "repairUpdate", "repairDelete"})
public class CheckDiffResult {
    private String schema;
    private String table;
    private String topic;
    private int partitions;
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
        topic = Objects.isNull(builder.getTopic()) ? "" : builder.getTopic();
        schema = Objects.isNull(builder.getSchema()) ? "" : builder.getSchema();
        createTime = builder.getCreateTime();
        if (builder.isExistTableMiss()) {
            initEmptyCollections();
            resultTableNotExist(builder.getOnlyExistEndpoint());
        } else if (builder.isTableStructureEquals()) {
            keyUpdateSet = builder.getKeyUpdateSet();
            keyInsertSet = builder.getKeyInsertSet();
            keyDeleteSet = builder.getKeyDeleteSet();
            repairUpdate = builder.getRepairUpdate();
            repairInsert = builder.getRepairInsert();
            repairDelete = builder.getRepairDelete();
            resultAnalysis();
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
        result = "failed";
        message = "table structure is not equals , please check the database sync !";
    }

    private void resultTableNotExist(Endpoint onlyExistEndpoint) {
        result = "failed";
        message =
            "table [".concat(table).concat("] , ").concat(" only exist in ").concat(onlyExistEndpoint.getDescription())
                     .concat("!");
    }

    private void resultAnalysis() {
        if (CollectionUtils.isEmpty(keyInsertSet) && CollectionUtils.isEmpty(keyUpdateSet) && CollectionUtils
            .isEmpty(keyDeleteSet)) {
            result = "success";
            message = schema.concat(".").concat(table).concat("_[").concat(String.valueOf(partitions))
                            .concat("] check success");
        } else {
            result = "failed";
            message =
                schema.concat(".").concat(table).concat("_[").concat(String.valueOf(partitions)).concat("] check : ")
                      .concat(" insert=" + keyInsertSet.size()).concat(" update=" + keyUpdateSet.size())
                      .concat(" delete=" + keyDeleteSet.size());
        }
    }
}
