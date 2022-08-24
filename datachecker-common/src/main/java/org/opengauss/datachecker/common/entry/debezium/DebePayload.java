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

package org.opengauss.datachecker.common.entry.debezium;

import lombok.Data;

import java.util.List;
import java.util.Map;

/**
 * DebePayload
 *
 * @author ：wangchao
 * @date ：Created in 2022/6/30
 * @since ：11
 */
@Data
public class DebePayload {
    private PayloadSource source;
    private Map<String, String> before;
    private Map<String, String> after;
    private String databaseName;
    private String schemaName;
    private String ddl;
    private List<PayloadTableChange> tableChanges;
}

@Data
class PayloadTableChange {
    private String id;
    private String type;
    private PayloadTable table;
}

@Data
class PayloadTable {
    private String defaultCharsetName;
    private List<String> primaryKeyColumnNames;
    private List<String> primaryKeyColumnChanges;
    private List<String> foreignKeyColumns;
    private List<String> uniqueColumns;
    private List<String> checkColumns;
    private List<PayloadTableColumns> columns;
    private String comment;
}

@Data
class PayloadTableColumns {
    private String name;
    private int jdbcType;
    private String nativeType;
    private String typeName;
    private String typeExpression;
    private String charsetName;
    private int length;
    private int scale;
    private int position;
    private boolean optional;
    private String defaultValueExpression;
    private boolean autoIncremented;
    private boolean generated;
    private String comment;
    private List<String> modifyKeys;
}