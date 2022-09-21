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
        TypeHandler binaryByteToString = (resultSet, columnLabel) -> byteToString(resultSet.getBytes(columnLabel));
        TypeHandler blobToString = (resultSet, columnLabel) -> blobToString(resultSet.getBlob(columnLabel));

        // byte binary blob
        typeHandlers.put(OpenGaussType.BYTEA, binaryByteToString);
        typeHandlers.put(OpenGaussType.BLOB, blobToString);

        // The openGauss jdbc driver obtains the character,character varying  type as varchar
        typeHandlers.put(OpenGaussType.VARCHAR, this::trim);
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
            return String.valueOf(resultSet.getObject(columnLabel));
        }
    }

    interface OpenGaussType {
        String BYTEA = "bytea";
        String BLOB = "blob";
        String VARCHAR = "varchar";
        String BPCHAR = "bpchar";
        String DATE = "date";
        String TIME = "time";
        String TIMESTAMP = "timestamp";
    }
}
