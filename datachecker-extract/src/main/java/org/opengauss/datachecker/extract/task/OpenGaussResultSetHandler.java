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

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

/**
 * OpenGaussResultSetHandler
 *
 * @author ：wangchao
 * @date ：Created in 2022/9/19
 * @since ：11
 */
public class OpenGaussResultSetHandler extends ResultSetHandler {
    private final Map<String, TypeHandler> typeHandlers = new ConcurrentHashMap<>();

    {
        TypeHandler byteaToString = (resultSet, columnLabel) -> bytesToString(resultSet.getBytes(columnLabel));
        TypeHandler blobToString = (resultSet, columnLabel) -> resultSet.getString(columnLabel);
        TypeHandler booleanToString = (resultSet, columnLabel) -> booleanToString(resultSet, columnLabel);
        TypeHandler numericToString = (resultSet, columnLabel) -> numericToString(resultSet.getBigDecimal(columnLabel));

        typeHandlers.put(OpenGaussType.NUMERIC, numericToString);

        // byte binary blob
        typeHandlers.put(OpenGaussType.BYTEA, byteaToString);
        typeHandlers.put(OpenGaussType.BLOB, blobToString);
        typeHandlers.put(OpenGaussType.BOOLEAN, booleanToString);

        // The openGauss jdbc driver obtains the character,character varying  type as varchar
        typeHandlers.put(OpenGaussType.BPCHAR, this::trim);

        // date time timestamp
        typeHandlers.put(OpenGaussType.DATE, this::getDateFormat);
        typeHandlers.put(OpenGaussType.TIME, this::getTimeFormat);
        typeHandlers.put(OpenGaussType.TIMESTAMP, this::getTimestampFormat);
    }

    @Override
    public String convert(ResultSet resultSet, String columnTypeName, String columnLabel) throws SQLException {
        if (typeHandlers.containsKey(columnTypeName)) {
            return typeHandlers.get(columnTypeName).convert(resultSet, columnLabel);
        } else {
            return Objects.isNull(resultSet.getObject(columnLabel)) ? NULL :
                String.valueOf(resultSet.getObject(columnLabel));
        }
    }

    protected String booleanToString(ResultSet rs, String columnLabel) throws SQLException {
        final int booleanVal = rs.getInt(columnLabel);
        return booleanVal == 1 ? "true" : "false";
    }

    @SuppressWarnings("all")
    interface OpenGaussType {
        String BYTEA = "bytea";
        String BOOLEAN = "bool";
        String BLOB = "blob";
        String NUMERIC = "numeric";
        String VARCHAR = "varchar";
        String BPCHAR = "bpchar";
        String DATE = "date";
        String TIME = "time";
        String TIMESTAMP = "timestamp";
    }
}
