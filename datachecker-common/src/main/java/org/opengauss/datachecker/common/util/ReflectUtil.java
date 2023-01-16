package org.opengauss.datachecker.common.util;

import lombok.extern.slf4j.Slf4j;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

/**
 * ReflectUtil
 *
 * @author ：wangchao
 * @date ：Created in 2023/1/14
 * @since ：11
 */
@Slf4j
public class ReflectUtil {

    /**
     * reflect invoke method, which is private method
     *
     * @param classz     classz
     * @param object     classz instance
     * @param methodName method name
     */
    public static void invoke(Class classz, Object object, String methodName) {
        try {
            Method method = classz.getDeclaredMethod(methodName);
            method.setAccessible(true);
            method.invoke(object);
        } catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException e) {
            log.error("call reflect method  {}.{} error.", classz, methodName);
        }
    }

    public static <T> T getField(Class classz, Object object, Class<T> t, String fieldName) {
        try {
            Field field = classz.getDeclaredField(fieldName);
            field.setAccessible(true);
            return (T) field.get(object);
        } catch (IllegalAccessException | NoSuchFieldException e) {
            log.error("get reflect field  {}.{} error.", classz, fieldName);
        }
        return null;
    }

    public static void setField(Class classz, Object object, String fieldName, Object value) {
        try {
            Field field = classz.getDeclaredField(fieldName);
            field.setAccessible(true);
            field.set(object, value);
        } catch (IllegalAccessException | NoSuchFieldException e) {
            log.error("get reflect field  {}.{} error.", classz, fieldName);
        }
    }
}
