package org.opengauss.datachecker.extract.controller;

import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.opengauss.datachecker.common.web.Result;
import org.opengauss.datachecker.extract.kafka.KafkaManagerService;
import org.opengauss.datachecker.extract.service.DataExtractService;
import org.opengauss.datachecker.extract.service.MetaDataService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;


@Tag(name = "Clearing the environment at the extraction endpoint")
@RestController
public class ExtractCleanController {

    @Autowired
    private MetaDataService metaDataService;

    @Autowired
    private DataExtractService dataExtractService;

    @Autowired
    private KafkaManagerService kafkaManagerService;

    /**
     * clear the endpoint information and reinitialize the environment.
     *
     * @return interface invoking result
     */
    @Operation(summary = "clear the endpoint information and reinitialize the environment")
    @PostMapping("/extract/clean/environment")
    Result<Void> cleanEnvironment(@RequestParam(name = "processNo") String processNo) {
        metaDataService.init();
        dataExtractService.cleanBuildedTask();
        kafkaManagerService.cleanKafka(processNo);
        return Result.success();
    }

    @Operation(summary = "clears the task cache information of the current ednpoint")
    @PostMapping("/extract/clean/task")
    Result<Void> cleanTask() {
        dataExtractService.cleanBuildedTask();
        return Result.success();
    }

    @Operation(summary = "clear the kafka information of the current endpoint")
    @PostMapping("/extract/clean/kafka")
    Result<Void> cleanKafka() {
        kafkaManagerService.cleanKafka();
        return Result.success();
    }
}
