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
    private LocalDateTime createTime;
    private Set<String> keyUpdateSet;
    private Set<String> keyInsertSet;
    private Set<String> keyDeleteSet;

    private List<String> repairUpdate;
    private List<String> repairInsert;
    private List<String> repairDelete;

    public AbstractCheckDiffResultBuilder(FeignClientService feignClient) {
        this.feignClient = feignClient;
    }

    /**
     * @return 返回构建器自身对象
     */
    protected abstract B self();

    /**
     * @return 执行构建器
     */
    public abstract C build();

    public B table(String table) {
        this.table = table;
        return this.self();
    }

    public B topic(String topic) {
        this.topic = topic;
        return this.self();
    }

    public B schema(String schema) {
        this.schema = schema;
        return this.self();
    }

    public B partitions(int partitions) {
        this.partitions = partitions;
        return this.self();
    }

    public B keyUpdateSet(Set<String> keyUpdateSet) {
        this.keyUpdateSet = keyUpdateSet;
        this.repairUpdate = checkRepairSinkDiff(DML.REPLACE, this.schema, this.table, this.keyUpdateSet);
        return this.self();
    }

    public B keyInsertSet(Set<String> keyInsertSet) {
        this.keyInsertSet = keyInsertSet;
        this.repairInsert = checkRepairSinkDiff(DML.INSERT, this.schema, this.table, this.keyInsertSet);
        return this.self();
    }

    public B keyDeleteSet(Set<String> keyDeleteSet) {
        this.keyDeleteSet = keyDeleteSet;
        this.repairDelete = checkRepairSinkDiff(DML.DELETE, this.schema, this.table, this.keyDeleteSet);
        return this.self();
    }


    public static AbstractCheckDiffResultBuilder<?, ?> builder(FeignClientService feignClient) {
        return new AbstractCheckDiffResultBuilderImpl(feignClient);
    }

    private static final class AbstractCheckDiffResultBuilderImpl extends AbstractCheckDiffResultBuilder<CheckDiffResult, AbstractCheckDiffResultBuilderImpl> {
        private AbstractCheckDiffResultBuilderImpl(FeignClientService feignClient) {
            super(feignClient);
        }

        @Override
        protected AbstractCheckDiffResultBuilderImpl self() {
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
