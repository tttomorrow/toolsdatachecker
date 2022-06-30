package org.opengauss.datachecker.extract.util;

import org.opengauss.datachecker.common.util.HashUtil;
import org.springframework.util.CollectionUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.opengauss.datachecker.extract.constants.ExtConstants.PRIMARY_DELIMITER;

/**
 * 哈希处理器，对查询结果进行哈希计算。
 * @author ：wangchao
 * @date ：Created in 2022/6/15
 * @since ：11
 */
public  class HashHandler {
    /**
     * 根据columns 集合中字段列表集合，在map中查找字段对应值，并对查找到的值进行拼接。
     *
     * @param columnsValueMap 字段对应查询数据
     * @param columns         字段名称列表
     * @return 当前Row对应的哈希计算结果
     */
    public long xx3Hash(Map<String, String> columnsValueMap, List<String> columns) {
        if (CollectionUtils.isEmpty(columns)) {
            return 0L;
        }
        StringBuffer sb = new StringBuffer();
        columns.forEach(colunm -> {
            if (columnsValueMap.containsKey(colunm)) {
                sb.append(columnsValueMap.get(colunm));
            }
        });
        return HashUtil.hashChars(sb.toString());
    }

    public String value(Map<String, String> columnsValueMap, List<String> columns) {
        if (CollectionUtils.isEmpty(columns)) {
            return "";
        }
        List<String> values = new ArrayList<>();
        columns.forEach(colunm -> {
            if (columnsValueMap.containsKey(colunm)) {
                values.add(columnsValueMap.get(colunm));
            }
        });
        return values.stream().map(String::valueOf).collect(Collectors.joining(PRIMARY_DELIMITER));
    }
}
