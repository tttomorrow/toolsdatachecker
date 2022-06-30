//package org.opengauss.datachecker.common.entry.extract;
//
//import lombok.Data;
//import lombok.experimental.Accessors;
//import lombok.experimental.SuperBuilder;
//
//import java.time.LocalDateTime;
//import java.util.List;
//import java.util.Set;
//
///**
// * @author ：wangchao
// * @date ：Created in 2022/6/18
// * @since ：11
// */
//@Data
//@Accessors(chain = true)
//public class CheckDiffResult {
//    private String table;
//    private int partitions;
//    private String topic;
//    private LocalDateTime createTime;
//
//    private Set<String> keyUpdateSet;
//    private Set<String> keyInsertSet;
//    private Set<String> keyDeleteSet;
//
//    private List<String> repairUpdate;
//    private List<String> repairInsert;
//    private List<String> repairDelete;
//
//    public CheckDiffResult(final CheckDiffResultBuilder<?, ?> b) {
//        this.table = b.table;
//        this.partitions = b.partitions;
//        this.topic = b.topic;
//        this.createTime = b.createTime;
//        this.keyUpdateSet = b.keyUpdateSet;
//        this.keyInsertSet = b.keyInsertSet;
//        this.keyDeleteSet = b.keyDeleteSet;
//        this.repairUpdate = b.repairUpdate;
//        this.repairInsert = b.repairInsert;
//        this.repairDelete = b.repairDelete;
//    }
//}
