package org.opengauss.datachecker.extract.controller;

import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.opengauss.datachecker.common.web.Result;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

@Tag(name = "ExtractHealthController", description = "health check of the data extraction service")
@RestController
public class ExtractHealthController {

    @Operation(summary = "data extraction health check")
    @GetMapping("/extract/health")
    public Result<Void> health() {
        return Result.success();
    }

}
