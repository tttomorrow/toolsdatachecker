package org.opengauss.datachecker.check.controller;

import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.opengauss.datachecker.check.modules.task.TaskManagerService;
import org.opengauss.datachecker.common.entry.enums.Endpoint;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.lang.NonNull;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import javax.validation.constraints.NotEmpty;
import java.util.List;

/**
 * @author ：wangchao
 * @date ：Created in 2022/5/25
 * @since ：11
 */
@Tag(name = "TaskStatusController", description = "校验服务-数据抽取任务状态")
@Validated
@RestController
public class TaskStatusController {

    @Autowired
    private TaskManagerService taskManagerService;

    /**
     * 刷新指定任务的数据抽取表执行状态
     *
     * @param tableName 表名称
     * @param endpoint 端点类型 {@link org.opengauss.datachecker.common.entry.enums.Endpoint}
     */
    @Operation(summary = "刷新指定任务的数据抽取任务执行状态")
    @PostMapping("/table/extract/status")
    public void refushTableExtractStatus(@Parameter(description = "表名称") @RequestParam(value = "tableName") @NotEmpty String tableName,
                                        @Parameter(description = "数据校验端点类型") @RequestParam(value = "endpoint") @NonNull Endpoint endpoint) {
        taskManagerService.refushTableExtractStatus(tableName, endpoint);
    }

    /**
     * 初始化任务状态
     *
     * @param tableNameList 表名称列表
     */
    @Operation(summary = "初始化任务状态")
    @PostMapping("/table/extract/status/init")
    public void initTableExtractStatus(@Parameter(description = "表名称列表") @RequestBody @NotEmpty List<String> tableNameList) {
        taskManagerService.initTableExtractStatus(tableNameList);
    }


}
