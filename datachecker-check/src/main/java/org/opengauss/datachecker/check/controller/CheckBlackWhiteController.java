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

package org.opengauss.datachecker.check.controller;

import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.opengauss.datachecker.check.service.CheckBlackWhiteService;
import org.opengauss.datachecker.common.web.Result;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

/**
 * @author ：wangchao
 * @date ：Created in 2022/5/25
 * @since ：11
 */
@Tag(name = "CheckBlackWhiteController", description = "Verification service - black and white list management")
@Validated
@RestController
@RequestMapping
public class CheckBlackWhiteController {

    @Autowired
    private CheckBlackWhiteService checkBlackWhiteService;

    /**
     * Add white list this function clears the historical white list and resets the white list to the current list
     *
     * @param whiteList whiteList
     * @return request result
     */
    @PostMapping("/add/white/list")
    public Result<Void> addWhiteList(
        @Parameter(name = "whiteList", description = "whiteList") @RequestBody List<String> whiteList) {
        checkBlackWhiteService.addWhiteList(whiteList);
        return Result.success();
    }

    /**
     * Update white list this function adds the current list to the white list on the basis of the current white list
     *
     * @param whiteList whiteList
     * @return request result
     */
    @PostMapping("/update/white/list")
    public Result<Void> updateWhiteList(
        @Parameter(name = "whiteList", description = "whiteList") @RequestBody List<String> whiteList) {
        checkBlackWhiteService.updateWhiteList(whiteList);
        return Result.success();
    }

    /**
     * Remove white list this function removes the current list from the current white list
     *
     * @param whiteList whiteList
     * @return request result
     */
    @PostMapping("/delete/white/list")
    public Result<Void> deleteWhiteList(
        @Parameter(name = "whiteList", description = "whiteList") @RequestBody List<String> whiteList) {
        checkBlackWhiteService.deleteWhiteList(whiteList);
        return Result.success();
    }

    /**
     * Query white list
     *
     * @return white list
     */
    @PostMapping("/query/white/list")
    public Result<List<String>> queryWhiteList() {
        return Result.success(checkBlackWhiteService.queryWhiteList());
    }

    /**
     * Add blacklist list this function clears the historical blacklist and resets the blacklist to the current list
     *
     * @param blackList blackList
     * @return request result
     */
    @PostMapping("/add/black/list")
    public Result<Void> addBlackList(
        @Parameter(name = "blackList", description = "Blacklist list") @RequestBody List<String> blackList) {
        checkBlackWhiteService.addBlackList(blackList);
        return Result.success();
    }

    /**
     * Update blacklist list this function adds the current list to the blacklist on the basis of the current blacklist
     *
     * @param blackList blackList
     * @return request result
     */
    @PostMapping("/update/black/list")
    public Result<Void> updateBlackList(
        @Parameter(name = "blackList", description = "Blacklist list") @RequestBody List<String> blackList) {
        checkBlackWhiteService.updateBlackList(blackList);
        return Result.success();
    }

    /**
     * Remove blacklist list this function removes the current list from the blacklist based on the current blacklist
     *
     * @param blackList blackList
     * @return request result
     */
    @PostMapping("/delete/black/list")
    public Result<Void> deleteBlackList(
        @Parameter(name = "blackList", description = "Blacklist list") @RequestBody List<String> blackList) {
        checkBlackWhiteService.deleteBlackList(blackList);
        return Result.success();
    }

    /**
     * Query blacklist list
     *
     * @return blackList
     */
    @Operation(summary = "Query blacklist list ")
    @PostMapping("/query/black/list")
    public Result<List<String>> queryBlackList() {
        return Result.success(checkBlackWhiteService.queryBlackList());
    }
}
