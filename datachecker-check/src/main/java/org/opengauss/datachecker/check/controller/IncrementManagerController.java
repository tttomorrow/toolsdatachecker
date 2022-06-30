package org.opengauss.datachecker.check.controller;

import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.opengauss.datachecker.check.service.IncrementManagerService;
import org.opengauss.datachecker.common.entry.extract.SourceDataLog;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

import javax.validation.constraints.NotEmpty;
import java.util.List;

/**
 * @author ：wangchao
 * @date ：Created in 2022/5/25
 * @since ：11
 */
@Tag(name = "IncrementManagerController", description = "校验服务-增量校验管理")
@Validated
@RestController
public class IncrementManagerController {

    @Autowired
    private IncrementManagerService incrementManagerService;

    /**
     * 增量校验日志通知
     *
     * @param dataLogList 增量校验日志
     */
    @Operation(summary = "增量校验日志通知")
    @PostMapping("/notify/source/increment/data/logs")
    public void notifySourceIncrementDataLogs(@Parameter(description = "增量校验日志")
                                              @RequestBody @NotEmpty List<SourceDataLog> dataLogList) {
        incrementManagerService.notifySourceIncrementDataLogs(dataLogList);
    }


}
