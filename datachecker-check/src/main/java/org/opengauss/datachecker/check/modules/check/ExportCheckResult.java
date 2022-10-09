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

package org.opengauss.datachecker.check.modules.check;

import lombok.extern.slf4j.Slf4j;
import org.opengauss.datachecker.common.util.FileUtils;
import org.opengauss.datachecker.common.util.JsonObjectUtil;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

/**
 * @author ：wangchao
 * @date ：Created in 2022/6/17
 * @since ：11
 */
@Slf4j
public class ExportCheckResult {
    private static final DateTimeFormatter FORMATTER_DIR = DateTimeFormatter.ofPattern("yyyyMMddHHmmssSSS");

    private static final String CHECK_RESULT_BAK_DIR = File.separator + "result_bak" + File.separator;
    private static final String CHECK_RESULT_PATH = File.separator + "result" + File.separator;

    public static void export(String path, CheckDiffResult result) {
        String fileName = getCheckResultFileName(path, result.getTopic(), result.getPartitions());
        FileUtils.deleteFile(fileName);
        FileUtils.writeFile(fileName, JsonObjectUtil.format(result));
    }

    private static String getCheckResultFileName(String path, String topicName, int partitions) {
        final String fileName = topicName.concat("_").concat(String.valueOf(partitions)).concat(".txt");
        return getResultPath(path).concat(fileName);
    }

    /**
     * Initialize the verification result environment
     *
     * @param path Verification result output path
     */
    public static void initEnvironment(String path) {
        String checkResultPath = getResultPath(path);
        FileUtils.createDirectories(checkResultPath);
        FileUtils.createDirectories(getResultBakRootDir(path));
        try {
            final String backDir = getResultBakDir(path);
            Files.move(Path.of(checkResultPath), Path.of(backDir));
            FileUtils.createDirectories(checkResultPath);
        } catch (IOException e) {
            log.error("initialize the verification result environment error");
        }
        log.info("initialize the verification result environment");
    }

    private static String getResultPath(String path) {
        return path.concat(CHECK_RESULT_PATH);
    }

    private static String getResultBakRootDir(String path) {
        return path.concat(CHECK_RESULT_BAK_DIR);
    }

    private static String getResultBakDir(String path) {
        return getResultBakRootDir(path).concat(FORMATTER_DIR.format(LocalDateTime.now()));
    }
}
