package org.opengauss.datachecker.check.controller;

import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.opengauss.datachecker.check.service.CheckService;
import org.opengauss.datachecker.common.entry.check.IncrementCheckConifg;
import org.opengauss.datachecker.common.entry.enums.CheckMode;
import org.opengauss.datachecker.common.web.Result;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.*;

/**
 * @author ：wangchao
 * @date ：Created in 2022/5/25
 * @since ：11
 */
@Tag(name = "CheckStartController", description = "校验服务-校验服务启动命令")
@Validated
@RestController
@RequestMapping
public class CheckStartController {

    @Autowired
    private CheckService checkService;

    /**
     * 开启校验
     */
    @Operation(summary = "开启校验")
    @PostMapping("/start/check")
    public Result<String> statCheck(@Parameter(name = "checkMode", description = CheckMode.API_DESCRIPTION)
                                    @RequestParam("checkMode") CheckMode checkMode) {
        return Result.success(checkService.start(checkMode));
    }

    @Operation(summary = "增量校验配置初始化")
    @PostMapping("/increment/check/config")
    public Result<Void> incrementCheckConifg(@RequestBody IncrementCheckConifg incrementCheckConifg) {
        checkService.incrementCheckConifg(incrementCheckConifg);
        return Result.success();
    }

    @Operation(summary = "停止校验服务 并 清理校验服务", description = "对当前进程中的校验状态，以及抽取的数据等相关信息进行全面清理。")
    @PostMapping("/stop/clean/check")
    public Result<Void> cleanCheck() {
        checkService.cleanCheck();
        return Result.success();
    }

    @Operation(summary = "查询当前校验服务进程编号")
    @GetMapping("/get/check/process")
    public Result<String> getCurrentCheckProcess() {
        return Result.success(checkService.getCurrentCheckProcess());
    }
}
