package org.opengauss.datachecker.extract.controller;

import io.swagger.v3.oas.annotations.Parameter;
import org.hibernate.validator.constraints.Range;
import org.opengauss.datachecker.extract.TableRowCount;
import org.opengauss.datachecker.extract.service.ExtractMockDataService;
import org.opengauss.datachecker.extract.service.ExtractMockTableService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;


@RestController
public class ExtractMockController {
    @Autowired
    private ExtractMockDataService extractMockDataService;
    @Autowired
    private ExtractMockTableService extractMockTableService;

    /**
     * 根据名称创建数据库表 表字段目前为固定字段
     *
     * @param tableName 创建表名
     * @return
     * @throws Exception
     */
    @PostMapping("/mock/createTable")
    public String createTable(@RequestParam("tableName") String tableName) throws Exception {
        return extractMockTableService.createTable(tableName);
    }

    @GetMapping("/mock/query/all/table/count")
    public List<TableRowCount> getAllTableCount() {
        return extractMockTableService.getAllTableCount();
    }

    /**
     * 向指定表名称，采用多线程方式批量插入指定数据量的Mock数据
     *
     * @param tableName   表名
     * @param totalCount  插入数据总量
     * @param threadCount 线程数 最大线程总数不能超过2000 ，超过2000可能会导致数据丢失
     * @return
     */
    @PostMapping("/batch/mock/data")
    public String batchMockData(@Parameter(name = "tableName", description = "待插入数据表名") @RequestParam("tableName") String tableName,
                                @Parameter(name = "totalCount", description = "待插入数据总量") @RequestParam("totalCount") long totalCount,
                                @Parameter(name = "threadCount", description = "多线程插入，设置线程总数")
                                @Range(min = 1, max = 30, message = "设置的线程总数必须在[1-30]之间")
                                @RequestParam("threadCount") int threadCount,
                                @Parameter(name = "createTable", description = "是否创建表") @RequestParam("createTable") boolean createTable) {
        try {
            if (createTable) {
                extractMockTableService.createTable(tableName);
            }
            extractMockDataService.batchMockData(tableName, totalCount, threadCount);
        } catch (Exception throwables) {
            System.err.println(throwables.getMessage());
        }
        return "OK";
    }
}
