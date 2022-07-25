package org.opengauss.datachecker.common.util;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.serializer.SerializerFeature;
import lombok.extern.slf4j.Slf4j;


/**
 * @author ：wangchao
 * @date ：Created in 2022/5/23
 * @since ：11
 */
@Slf4j
public class JsonObjectUtil {

    /**
     * 对象格式化为JSON字符串，格式化根据属性进行自动换行<p>
     * {@code  SerializerFeature.PrettyFormat}<p>
     * {@code  SerializerFeature.WriteMapNullValue} 空指针格式化<p>
     * {@code  SerializerFeature.WriteDateUseDateFormat} 日期格式化<p>
     *
     * @param object 格式化对象
     * @return 格式化字符串
     */
    public static String format(Object object) {
        return JSONObject.toJSONString(object, SerializerFeature.PrettyFormat, SerializerFeature.WriteMapNullValue,
                SerializerFeature.WriteDateUseDateFormat);
    }
}
