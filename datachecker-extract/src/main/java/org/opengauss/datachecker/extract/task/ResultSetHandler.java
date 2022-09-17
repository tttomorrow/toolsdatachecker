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

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.opengauss.datachecker.common.constant.Constants.InitialCapacity;
import org.springframework.lang.NonNull;

import java.sql.Date;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.format.DateTimeFormatter;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.TimeZone;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.IntStream;

/**
 * Result set object processor
 *
 * @author wang chao
 * @date ：Created in 2022/6/13
 * @since 11
 **/
@Slf4j
public class ResultSetHandler {
    private static final DateTimeFormatter DATE = DateTimeFormatter.ofPattern("yyyy-MM-dd");
    private static final DateTimeFormatter TIME = DateTimeFormatter.ofPattern("HH:mm:ss");
    private static final DateTimeFormatter TIMESTAMP = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS");
    private static final String EMPTY = "";

    private Map<Integer, TypeHandler> typeHandlers = new ConcurrentHashMap<>();

    {
        TypeHandler binaryByteToString = (resultSet, columnLabel) -> byteToString(resultSet.getBytes(columnLabel));

        // byte binary blob
        typeHandlers.put(Types.BINARY, binaryByteToString);
        typeHandlers.put(Types.VARBINARY, binaryByteToString);
        typeHandlers.put(Types.LONGVARBINARY, binaryByteToString);
        typeHandlers.put(Types.BLOB, binaryByteToString);

        // date time timestamp
        typeHandlers.put(Types.DATE, this::getDateFormat);
        typeHandlers.put(Types.TIME, this::getTimeFormat);
        typeHandlers.put(Types.TIME_WITH_TIMEZONE, this::getTimeFormat);
        typeHandlers.put(Types.TIMESTAMP, this::getTimestampFormat);
        typeHandlers.put(Types.TIMESTAMP_WITH_TIMEZONE, this::getTimestampFormat);
    }

    /**
     * Convert the current query result set into map according to the metadata information of the result set
     *
     * @param resultSet JDBC Data query result set
     * @return JDBC Data encapsulation results
     */
    public Map<String, String> putOneResultSetToMap(ResultSet resultSet) {
        Map<String, String> values = new HashMap<>(InitialCapacity.CAPACITY_64);
        try {
            final ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
            IntStream.rangeClosed(1, resultSetMetaData.getColumnCount()).forEach(columnIdx -> {
                try {
                    // Get the column and its corresponding column name
                    String columnLabel = resultSetMetaData.getColumnLabel(columnIdx);
                    // Get the corresponding value from the result set according to the column name
                    final int columnType = resultSetMetaData.getColumnType(columnIdx);
                    if (typeHandlers.containsKey(columnType)) {
                        values.put(columnLabel, typeHandlers.get(columnType).convert(resultSet, columnLabel));
                    } else {
                        values.put(columnLabel, String.valueOf(resultSet.getObject(columnLabel)));
                    }
                } catch (SQLException ex) {
                    log.error("putOneResultSetToMap Convert data according to result set metadata information.", ex);
                }
            });
        } catch (SQLException ex) {
            log.error("putOneResultSetToMap get data metadata information exception", ex);
        }
        return values;
    }

    private String getDateFormat(@NonNull ResultSet resultSet, String columnLabel) throws SQLException {
        String formatTime = StringUtils.EMPTY;
        final Date date = resultSet.getDate(columnLabel);
        if (Objects.nonNull(date)) {
            formatTime = DATE.format(date.toLocalDate());
        }
        return formatTime;
    }

    private String getTimeFormat(@NonNull ResultSet resultSet, String columnLabel) throws SQLException {
        String formatTime = StringUtils.EMPTY;
        final Time time = resultSet.getTime(columnLabel);
        if (Objects.nonNull(time)) {
            formatTime = TIME.format(time.toLocalTime());
        }
        return formatTime;
    }

    private String getTimestampFormat(@NonNull ResultSet resultSet, String columnLabel) throws SQLException {
        String formatTime = StringUtils.EMPTY;
        final Timestamp timestamp =
            resultSet.getTimestamp(columnLabel, Calendar.getInstance(TimeZone.getTimeZone("GMT+8")));
        if (Objects.nonNull(timestamp)) {
            formatTime = TIMESTAMP.format(timestamp.toLocalDateTime());
        }
        return formatTime;
    }

    private String byteToString(byte[] bytes) {
        if (bytes == null) {
            return EMPTY;
        }
        int iMax = bytes.length - 1;
        if (iMax == -1) {
            return EMPTY;
        }
        StringBuilder builder = new StringBuilder();
        for (int i = 0; ; i++) {
            builder.append(bytes[i]);
            if (i == iMax) {
                return builder.toString();
            }
            builder.append(",");
        }
    }

    @FunctionalInterface
    interface TypeHandler {
        /**
         * result convert to string
         *
         * @param resultSet   resultSet
         * @param columnLabel columnLabel
         * @return result
         * @throws SQLException
         */
        String convert(ResultSet resultSet, String columnLabel) throws SQLException;
    }
}