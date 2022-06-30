package org.opengauss.datachecker.extract.util;

import org.opengauss.datachecker.common.entry.extract.ColumnsMetaData;
import org.opengauss.datachecker.common.entry.extract.TableMetadata;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * @author ：wangchao
 * @date ：Created in 2022/6/15
 * @since ：11
 */
public class MetaDataUtil {
    public static List<String> getTableColumns(TableMetadata tableMetadata) {
        if (Objects.isNull(tableMetadata)) {
            return emptyList();
        }
        List<ColumnsMetaData> columnsMetas = tableMetadata.getColumnsMetas();
        return getTableColumns(columnsMetas);
    }

    public static List<String> getTablePrimaryColumns(TableMetadata tableMetadata) {
        if (Objects.isNull(tableMetadata)) {
            return emptyList();
        }
        List<ColumnsMetaData> primaryMetas = tableMetadata.getPrimaryMetas();
        return getTableColumns(primaryMetas);
    }

    private static ArrayList<String> emptyList() {
        return new ArrayList<>(0);
    }

    private static List<String> getTableColumns(List<ColumnsMetaData> columnsMetas) {
        if (Objects.isNull(columnsMetas)) {
            return emptyList();
        }
        return columnsMetas.stream()
                .sorted(Comparator.comparing(ColumnsMetaData::getOrdinalPosition))
                .map(ColumnsMetaData::getColumnName)
                .collect(Collectors.toList());
    }
}
