package org.opengauss.datachecker.common.exception;


/**
 * 数据抽取服务，表元数据信息不存在
 *
 * @author ：wangchao
 * @date ：Created in 2022/5/23
 * @since ：11
 */
public class TableNotExistException extends ExtractException {
    private static final String ERROR_MESSAGE = "table of Meatedata [%s] is not exist!";


    public TableNotExistException(String tableName) {
        super(String.format(ERROR_MESSAGE, tableName));
    }

}
