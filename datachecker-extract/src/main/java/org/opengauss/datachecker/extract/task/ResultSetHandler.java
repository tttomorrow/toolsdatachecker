package org.opengauss.datachecker.extract.task;

/**
 * @author ：wangchao
 * @date ：Created in 2022/6/13
 * @since ：11
 */

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.lang.NonNull;

import java.sql.*;
import java.sql.Date;
import java.text.SimpleDateFormat;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.stream.IntStream;

/**
 * @author wang chao
 * @description 结果集对象处理器
 * @since 11
 **/
@Slf4j
public class ResultSetHandler {

    private final ObjectMapper mapper = ObjectMapperWapper.getObjectMapper();
    private static final List<Integer> SQL_TIME_TYPES = List.of(Types.DATE, Types.TIME, Types.TIMESTAMP, Types.TIME_WITH_TIMEZONE, Types.TIMESTAMP_WITH_TIMEZONE);

    private static final DateTimeFormatter DATE = DateTimeFormatter.ofPattern("yyyy-MM-dd");
    private static final DateTimeFormatter TIME = DateTimeFormatter.ofPattern("HH:mm:ss");
    private static final DateTimeFormatter TIMESTAMP = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS");

    /**
     * 将当前查询结果集 根据结果集元数据信息转换为Map
     *
     * @param resultSet JDBC 数据查询结果集
     * @param rsmd      JDBC 结果集元数据
     * @return JDBC 数据封装结果
     * @throws SQLException 返回SQL异常
     */
    public Map<String, String> putOneResultSetToMap(ResultSet resultSet, ResultSetMetaData rsmd) throws SQLException {
        Map<String, String> values = new HashMap<String, String>();

        IntStream.range(0, rsmd.getColumnCount()).forEach(idx -> {
            try {
                int columnIdx = idx + 1;
                // 获取列及对应的列名
                String columnLabel = rsmd.getColumnLabel(columnIdx);
                // 根据列名从ResultSet结果集中获得对应的值
                Object columnValue;

                final int columnType = rsmd.getColumnType(columnIdx);
                if (SQL_TIME_TYPES.contains(columnType)) {
                    columnValue = timeHandler(resultSet, columnIdx, columnType);
                } else {
                    columnValue = resultSet.getObject(columnLabel);
                }
                // 列名为key,列的值为value
                values.put(columnLabel, mapper.convertValue(columnValue, String.class));

            } catch (SQLException ex) {
                log.error("putOneResultSetToMap 根据结果集元数据信息转换数据结果集异常 {}", ex.getMessage());
            }
        });
        return values;
    }

    private String timeHandler(ResultSet resultSet, int columnIdx, int columnType) throws SQLException {
        String format = StringUtils.EMPTY;
        switch (columnType) {
            case Types.DATE:
                format = getDateFormat(resultSet, columnIdx);
                break;
            case Types.TIME:
            case Types.TIME_WITH_TIMEZONE:
                format = getTimeFormat(resultSet, columnIdx);
                break;
            case Types.TIMESTAMP:
            case Types.TIMESTAMP_WITH_TIMEZONE:
                format = getTimestampFormat(resultSet, columnIdx);
                break;
            default:
        }
        return format;
    }

    private String getDateFormat(@NonNull ResultSet resultSet, int columnIdx) throws SQLException {
        String formatTime = StringUtils.EMPTY;
        final Date date = resultSet.getDate(columnIdx);
        if (Objects.nonNull(date)) {
            formatTime = DATE.format(date.toLocalDate());
        }
        return formatTime;
    }

    private String getTimeFormat(@NonNull ResultSet resultSet, int columnIdx) throws SQLException {
        String formatTime = StringUtils.EMPTY;
        final Time time = resultSet.getTime(columnIdx);
        if (Objects.nonNull(time)) {
            formatTime = TIME.format(time.toLocalTime());
        }
        return formatTime;
    }

    private String getTimestampFormat(@NonNull ResultSet resultSet, int columnIdx) throws SQLException {
        String formatTime = StringUtils.EMPTY;
        final Timestamp timestamp = resultSet.getTimestamp(columnIdx, Calendar.getInstance(TimeZone.getTimeZone("GMT+8")));
        if (Objects.nonNull(timestamp)) {
            formatTime = TIMESTAMP.format(timestamp.toLocalDateTime());
        }
        return formatTime;
    }
    /**
     * 结果集对象处理器 将结果集数据转换为JSON字符串
     */
    static class ObjectMapperWapper {

        private static final ObjectMapper MAPPER;

        public static ObjectMapper getObjectMapper() {
            return MAPPER;
        }

        static {
            //创建ObjectMapper对象
            MAPPER = new ObjectMapper();

            //configure方法 配置一些需要的参数
            // 转换为格式化的json 显示出来的格式美化
            MAPPER.enable(SerializationFeature.INDENT_OUTPUT);

            //序列化的时候序列对象的那些属性
            //JsonInclude.Include.NON_DEFAULT 属性为默认值不序列化
            //JsonInclude.Include.ALWAYS      所有属性
            //JsonInclude.Include.NON_EMPTY   属性为 空（“”） 或者为 NULL 都不序列化
            //JsonInclude.Include.NON_NULL    属性为NULL 不序列化
            MAPPER.setSerializationInclusion(JsonInclude.Include.ALWAYS);

            //反序列化时,遇到未知属性会不会报错
            //true - 遇到没有的属性就报错 false - 没有的属性不会管，不会报错
            MAPPER.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

            //如果是空对象的时候,不抛异常
            MAPPER.configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false);

            //修改序列化后日期格式
            MAPPER.configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false);
            MAPPER.setDateFormat(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss"));
            //处理不同的时区偏移格式
            MAPPER.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
            MAPPER.registerModule(new JavaTimeModule());

        }
    }
}