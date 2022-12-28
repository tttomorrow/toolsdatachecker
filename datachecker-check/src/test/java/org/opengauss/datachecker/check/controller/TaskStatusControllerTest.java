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

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.opengauss.datachecker.check.modules.task.TaskManagerService;
import org.opengauss.datachecker.common.entry.enums.Endpoint;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;

import java.util.HashMap;
import java.util.Map;

import static org.mockito.BDDMockito.given;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/**
 * TaskStatusControllerTest
 *
 * @author ：wangchao
 * @date ：Created in 2022/7/20
 * @since ：11
 */
@WebMvcTest(TaskStatusController.class)
class TaskStatusControllerTest {
    @Autowired
    private MockMvc mockMvc;

    @MockBean
    private TaskManagerService taskManagerService;

    @DisplayName("refresh table extract status source ")
    @Test
    void testRefreshTableExtractStatus() throws Exception {
        this.taskManagerService.refreshTableExtractStatus("tableName", Endpoint.SOURCE, 1);
        // Run the test
        mockMvc.perform(
            post("/table/extract/status").param("tableName", "tableName").param("endpoint", Endpoint.SOURCE.name())
                                         .param("status", "1").accept(MediaType.APPLICATION_JSON))
               .andExpect(status().isOk());
    }

    @DisplayName("refresh table extract status sink ")
    @Test
    void testRefreshTableExtractStatus_Sink() throws Exception {

        // Run the test
        mockMvc.perform(
            post("/table/extract/status").param("tableName", "tableName").param("endpoint", Endpoint.SINK.name())
                                         .param("status", "1").accept(MediaType.APPLICATION_JSON))
               .andExpect(status().isOk());
    }

    @DisplayName("query table extract status")
    @Test
    void testQueryTableCheckStatus() throws Exception {
        final Map<String, Integer> tableStatus = new HashMap<>();
        tableStatus.put("table1", 7);
        given(this.taskManagerService.queryTableCheckStatus()).willReturn(tableStatus);
        mockMvc.perform(get("/query/all/table/status").accept(MediaType.APPLICATION_JSON)).andExpect(status().isOk())
               .andExpect(content().json("{table1=7}"));
    }
}
