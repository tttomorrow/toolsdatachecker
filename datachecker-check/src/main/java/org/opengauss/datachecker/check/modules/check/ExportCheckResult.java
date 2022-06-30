package org.opengauss.datachecker.check.modules.check;

import org.opengauss.datachecker.common.util.FileUtils;
import org.opengauss.datachecker.common.util.JsonObjectUtil;

/**
 * @author ：wangchao
 * @date ：Created in 2022/6/17
 * @since ：11
 */
public class ExportCheckResult {

    public static void export(String path, CheckDiffResult result) {
        FileUtils.createDirectories(path);
        String fileName = getCheckResultFileName(path, result.getTable(), result.getPartitions());
        FileUtils.writeAppendFile(fileName, JsonObjectUtil.format(result));
    }

    private static String getCheckResultFileName(String path, String tableName, int partitions) {
        return path.concat(tableName).concat("_").concat(String.valueOf(partitions)).concat(".txt");
    }
}
