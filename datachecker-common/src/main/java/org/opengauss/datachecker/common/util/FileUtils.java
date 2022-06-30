package org.opengauss.datachecker.common.util;

import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.List;
import java.util.Set;

/**
 * @author ：wangchao
 * @date ：Created in 2022/5/23
 * @since ：11
 */
@Slf4j
public class FileUtils {

    public static void createDirectories(String path) {
        File file = new File(path);
        if (!file.exists()) {
            try {
                Files.createDirectories(Paths.get(path));
            } catch (IOException e) {
                log.error("createDirectories error:", e);
            }
        }
    }

    public static void writeAppendFile(String filename, List<String> content) {
        try {
            Files.write(Paths.get(filename), content, StandardOpenOption.APPEND, StandardOpenOption.CREATE);
        } catch (IOException e) {
            log.error("file write error:", e);
        }
    }


    public static void writeAppendFile(String filename, Set<String> content) {
        try {
            Files.write(Paths.get(filename), content, StandardOpenOption.APPEND, StandardOpenOption.CREATE);
        } catch (IOException e) {
            log.error("file write error:", e);
        }
    }

    public static void writeAppendFile(String filename, String content) {
        try {
            Files.write(Paths.get(filename), content.getBytes(StandardCharsets.UTF_8), StandardOpenOption.APPEND, StandardOpenOption.CREATE);
        } catch (IOException e) {
            log.error("file write error:", e);
        }
    }
}
