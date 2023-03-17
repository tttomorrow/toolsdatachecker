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

package org.opengauss.datachecker.extract.task;

import com.mysql.cj.MysqlType;
import org.opengauss.datachecker.common.util.HexUtil;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

/**
 * MysqlResultSetHandler
 *
 * @author ：wangchao
 * @date ：Created in 2022/9/19
 * @since ：11
 */
public class MysqlResultSetHandler extends ResultSetHandler {
    private final Map<MysqlType, TypeHandler> typeHandlers = new ConcurrentHashMap<>();

    {
        TypeHandler binaryToString = (resultSet, columnLabel) -> byteToStringTrim(resultSet.getBytes(columnLabel));
        TypeHandler varbinaryToString = (resultSet, columnLabel) -> bytesToString(resultSet.getBytes(columnLabel));
        TypeHandler blobToString = (resultSet, columnLabel) -> HexUtil.byteToHexTrim(resultSet.getBytes(columnLabel));
        TypeHandler numericToString = (resultSet, columnLabel) -> numericToString(resultSet.getBigDecimal(columnLabel));
        TypeHandler bitBooleanToString = (resultSet, columnLabel) -> booleanToString(resultSet.getBoolean(columnLabel));

        typeHandlers.put(MysqlType.FLOAT, numericToString);
        typeHandlers.put(MysqlType.DOUBLE, numericToString);
        typeHandlers.put(MysqlType.DECIMAL, numericToString);
        typeHandlers.put(MysqlType.BIT, bitBooleanToString);
        // byte binary blob
        typeHandlers.put(MysqlType.BINARY, binaryToString);
        typeHandlers.put(MysqlType.VARBINARY, varbinaryToString);

        typeHandlers.put(MysqlType.BLOB, blobToString);
        typeHandlers.put(MysqlType.LONGBLOB, blobToString);
        typeHandlers.put(MysqlType.MEDIUMBLOB, blobToString);
        typeHandlers.put(MysqlType.TINYBLOB, blobToString);

        // date time timestamp
        typeHandlers.put(MysqlType.DATE, this::getDateFormat);
        typeHandlers.put(MysqlType.DATETIME, this::getTimestampFormat);
        typeHandlers.put(MysqlType.TIME, this::getTimeFormat);
        typeHandlers.put(MysqlType.TIMESTAMP, this::getTimestampFormat);
        typeHandlers.put(MysqlType.YEAR, this::getYearFormat);
    }

    private String byteToStringTrim(byte[] bytes) {
        return HexUtil.byteToHexTrim(bytes);
    }

    protected String booleanToString(Boolean bool) {
        if (Objects.isNull(bool)) {
            return NULL;
        }
        return String.valueOf(bool);
    }

    @Override
    public String convert(ResultSet resultSet, String columnTypeName, String columnLabel) throws SQLException {
        final MysqlType mysqlType = MysqlType.getByName(columnTypeName);
        if (typeHandlers.containsKey(mysqlType)) {
            return typeHandlers.get(mysqlType).convert(resultSet, columnLabel);
        } else {
            return Objects.isNull(resultSet.getObject(columnLabel)) ? NULL :
                String.valueOf(resultSet.getObject(columnLabel));
        }
    }
}