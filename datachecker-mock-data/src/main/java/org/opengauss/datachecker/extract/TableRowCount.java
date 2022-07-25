package org.opengauss.datachecker.extract;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.experimental.Accessors;

/**
 * @author ：wangchao
 * @date ：Created in 2022/6/6
 * @since ：11
 */
@Data
@AllArgsConstructor
public class TableRowCount {
    private String tableName;
    private long count;
}
