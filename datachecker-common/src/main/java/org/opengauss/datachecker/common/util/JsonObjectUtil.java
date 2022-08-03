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

package org.opengauss.datachecker.common.util;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.serializer.SerializerFeature;
import lombok.extern.slf4j.Slf4j;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

/**
 * @author ：wangchao
 * @date ：Created in 2022/5/23
 * @since ：11
 */
@Slf4j
public class JsonObjectUtil {

    private static final DateTimeFormatter FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS");

    /**
     * The object is formatted as a JSON string,
     * and the formatting is automatically wrapped according to the attributes
     * {@code  SerializerFeature.PrettyFormat}<p>
     * {@code  SerializerFeature.WriteMapNullValue} Null pointer formatting<p>
     * {@code  SerializerFeature.WriteDateUseDateFormat} date format<p>
     *
     * @param object Formatting Objects
     * @return formatting string
     */
    public static String format(Object object) {
        return JSONObject.toJSONString(object, SerializerFeature.PrettyFormat, SerializerFeature.WriteMapNullValue,
            SerializerFeature.WriteDateUseDateFormat);
    }

    /**
     * Localdatetime time is formatted as yyyy-MM-dd HH:mm:ss.SSS
     *
     * @param localDateTime time
     * @return time of string
     */
    public static String formatTime(LocalDateTime localDateTime) {
        return FORMATTER.format(localDateTime);
    }
}
