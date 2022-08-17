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

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.opengauss.datachecker.check.modules.task.TaskManagerService;
import org.opengauss.datachecker.common.entry.enums.Endpoint;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.mock.web.MockHttpServletResponse;
import org.springframework.test.context.junit.jupiter.SpringExtension;
import org.springframework.test.web.servlet.MockMvc;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.verify;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;

/**
 * TaskStatusControllerTest
 *
 * @author ：wangchao
 * @date ：Created in 2022/7/20
 * @since ：11
 */
@ExtendWith(SpringExtension.class)
@WebMvcTest(TaskStatusController.class)
class TaskStatusControllerTest {
    @Autowired
    private MockMvc mockMvc;

    @MockBean
    private TaskManagerService taskManagerService;

    @Test
    void testRefushTaskExtractStatus() throws Exception {
        // Setup
        // Run the test
        final MockHttpServletResponse response = mockMvc.perform(
            post("/table/extract/status").param("tableName", "tableName").param("endpoint", Endpoint.SOURCE.name())
                                         .accept(MediaType.APPLICATION_JSON)).andReturn().getResponse();

        // Verify the results
        assertThat(response.getStatus()).isEqualTo(HttpStatus.OK.value());
        assertThat(response.getContentAsString()).isEqualTo("");
        verify(taskManagerService).refreshTableExtractStatus("tableName", Endpoint.SOURCE);
    }
}
