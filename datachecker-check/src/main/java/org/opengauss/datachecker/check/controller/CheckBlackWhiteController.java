package org.opengauss.datachecker.check.controller;

import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.opengauss.datachecker.check.service.CheckBlackWhiteService;
import org.opengauss.datachecker.check.service.CheckService;
import org.opengauss.datachecker.common.entry.enums.CheckMode;
import org.opengauss.datachecker.common.web.Result;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.*;

import java.util.List;

/**
 * @author ：wangchao
 * @date ：Created in 2022/5/25
 * @since ：11
 */
@Tag(name = "CheckBlackWhiteController", description = "校验服务-黑白名单管理")
@Validated
@RestController
@RequestMapping
public class CheckBlackWhiteController {

    @Autowired
    private CheckBlackWhiteService checkBlackWhiteService;

    /**
     * 开启校验
     */
    @Operation(summary = "添加白名单列表 该功能清理历史白名单，重置白名单为当前列表")
    @PostMapping("/add/white/list")
    public Result<Void> addWhiteList(@Parameter(name = "whiteList", description = "白名单列表")
                                     @RequestBody List<String> whiteList) {
        checkBlackWhiteService.addWhiteList(whiteList);
        return Result.success();
    }

    @Operation(summary = "更新白名单列表 该功能在当前白名单基础上新增当前列表到白名单")
    @PostMapping("/update/white/list")
    public Result<Void> updateWhiteList(@Parameter(name = "whiteList", description = "白名单列表")
                                        @RequestBody List<String> whiteList) {
        checkBlackWhiteService.updateWhiteList(whiteList);
        return Result.success();
    }

    @Operation(summary = "移除白名单列表 该功能在当前白名单基础上移除当前列表到白名单")
    @PostMapping("/delete/white/list")
    public Result<Void> deleteWhiteList(@Parameter(name = "whiteList", description = "白名单列表")
                                        @RequestBody List<String> whiteList) {
        checkBlackWhiteService.deleteWhiteList(whiteList);
        return Result.success();
    }

    @Operation(summary = "查询白名单列表 ")
    @PostMapping("/query/white/list")
    public Result<List<String>> queryWhiteList() {
        return Result.success(checkBlackWhiteService.queryWhiteList());
    }

    @Operation(summary = "添加黑名单列表 该功能清理历史黑名单，重置黑名单为当前列表")
    @PostMapping("/add/black/list")
    public Result<Void> addBlackList(@Parameter(name = "blackList", description = "黑名单列表")
                                     @RequestBody List<String> blackList) {
        checkBlackWhiteService.addBlackList(blackList);
        return Result.success();
    }

    @Operation(summary = "更新黑名单列表 该功能在当前黑名单基础上新增当前列表到黑名单")
    @PostMapping("/update/black/list")
    public Result<Void> updateBlackList(@Parameter(name = "blackList", description = "黑名单列表")
                                        @RequestBody List<String> blackList) {
        checkBlackWhiteService.updateBlackList(blackList);
        return Result.success();
    }

    @Operation(summary = "移除黑名单列表 该功能在当前黑名单基础上移除当前列表到黑名单")
    @PostMapping("/delete/black/list")
    public Result<Void> deleteBlackList(@Parameter(name = "blackList", description = "黑名单列表")
                                        @RequestBody List<String> blackList) {
        checkBlackWhiteService.deleteBlackList(blackList);
        return Result.success();
    }

    @Operation(summary = "查询黑名单列表 ")
    @PostMapping("/query/black/list")
    public Result<List<String>> queryBlackList() {
        return Result.success(checkBlackWhiteService.queryBlackList());
    }
}
