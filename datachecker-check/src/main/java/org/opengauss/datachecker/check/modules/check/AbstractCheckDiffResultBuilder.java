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

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.opengauss.datachecker.check.client.FeignClientService;
import org.opengauss.datachecker.common.entry.enums.DML;
import org.opengauss.datachecker.common.entry.enums.Endpoint;
import org.opengauss.datachecker.common.exception.DispatchClientException;
import org.springframework.util.CollectionUtils;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * AbstractCheckDiffResultBuilder
 *
 * @author ：wangchao
 * @date ：Created in 2022/6/18
 * @since ：11
 */
@Slf4j
@Getter
public abstract class AbstractCheckDiffResultBuilder<C extends CheckDiffResult, B extends AbstractCheckDiffResultBuilder<C, B>> {
    private final FeignClientService feignClient;

    private String table;
    private int partitions;
    private String topic;
    private String schema;
    private boolean isTableStructureEquals;
    private LocalDateTime createTime;
    private Set<String> keyUpdateSet;
    private Set<String> keyInsertSet;
    private Set<String> keyDeleteSet;
    private List<String> repairUpdate;
    private List<String> repairInsert;
    private List<String> repairDelete;

    /**
     * construct
     *
     * @param feignClient feignClient
     */
    public AbstractCheckDiffResultBuilder(FeignClientService feignClient) {
        this.feignClient = feignClient;
    }

    /**
     * the builder's own abstract method
     *
     * @return Return the builder's own object
     */
    protected abstract B self();

    /**
     * Execution builder abstract method
     *
     * @return Execution builder
     */
    public abstract C build();

    /**
     * Set the table properties of the builder
     *
     * @param table table name
     * @return CheckDiffResultBuilder
     */
    public B table(String table) {
        this.table = table;
        return self();
    }

    /**
     * Set the table is TableStructureEquals
     *
     * @param isTableStructureEquals table is TableStructureEquals
     * @return CheckDiffResultBuilder
     */
    public B isTableStructureEquals(boolean isTableStructureEquals) {
        this.isTableStructureEquals = isTableStructureEquals;
        return self();
    }

    /**
     * Set the topic properties of the builder
     *
     * @param topic topic name
     * @return CheckDiffResultBuilder
     */
    public B topic(String topic) {
        this.topic = topic;
        return self();
    }

    /**
     * Set the schema properties of the builder
     *
     * @param schema schema
     * @return CheckDiffResultBuilder
     */
    public B schema(String schema) {
        this.schema = schema;
        return self();
    }

    /**
     * Set the partitions properties of the builder
     *
     * @param partitions partitions
     * @return CheckDiffResultBuilder
     */
    public B partitions(int partitions) {
        this.partitions = partitions;
        return self();
    }

    /**
     * Set the keyUpdateSet properties of the builder
     *
     * @param keyUpdateSet keyUpdateSet
     * @return CheckDiffResultBuilder
     */
    public B keyUpdateSet(Set<String> keyUpdateSet) {
        this.keyUpdateSet = keyUpdateSet;
        repairUpdate = checkRepairSinkDiff(DML.REPLACE, schema, table, this.keyUpdateSet);
        return self();
    }

    /**
     * Set the keyInsertSet properties of the builder
     *
     * @param keyInsertSet keyInsertSet
     * @return CheckDiffResultBuilder
     */
    public B keyInsertSet(Set<String> keyInsertSet) {
        this.keyInsertSet = keyInsertSet;
        repairInsert = checkRepairSinkDiff(DML.INSERT, schema, table, this.keyInsertSet);
        return self();
    }

    /**
     * Set the keyDeleteSet properties of the builder
     *
     * @param keyDeleteSet keyDeleteSet
     * @return CheckDiffResultBuilder
     */
    public B keyDeleteSet(Set<String> keyDeleteSet) {
        this.keyDeleteSet = keyDeleteSet;
        repairDelete = checkRepairSinkDiff(DML.DELETE, schema, table, this.keyDeleteSet);
        return self();
    }

    /**
     * build CheckDiffResultBuilder
     *
     * @param feignClient feignClient
     * @return CheckDiffResultBuilder
     */
    public static AbstractCheckDiffResultBuilder<?, ?> builder(FeignClientService feignClient) {
        return new CheckDiffResultBuilder(feignClient);
    }

    /**
     * CheckDiffResultBuilder
     */
    public static final class CheckDiffResultBuilder
        extends AbstractCheckDiffResultBuilder<CheckDiffResult, CheckDiffResultBuilder> {
        private CheckDiffResultBuilder(FeignClientService feignClient) {
            super(feignClient);
        }

        @Override
        protected CheckDiffResultBuilder self() {
            return this;
        }

        @Override
        public CheckDiffResult build() {
            super.createTime = LocalDateTime.now();
            return new CheckDiffResult(this);
        }
    }

    private List<String> checkRepairSinkDiff(DML dml, String schema, String tableName, Set<String> sinkDiffSet) {
        if (!CollectionUtils.isEmpty(sinkDiffSet)) {
            try {
                return feignClient.buildRepairDml(Endpoint.SOURCE, schema, tableName, dml, sinkDiffSet);
            } catch (DispatchClientException exception) {
                log.error("check table[{}] Repair [{}] Diff build Repair DML Error", tableName, dml, exception);
                return new ArrayList<>();
            }
        } else {
            return new ArrayList<>();
        }
    }
}
