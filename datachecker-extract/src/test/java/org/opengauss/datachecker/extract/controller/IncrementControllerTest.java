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

package org.opengauss.datachecker.extract.controller;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.opengauss.datachecker.common.entry.enums.Endpoint;
import org.opengauss.datachecker.common.util.ReflectUtil;
import org.opengauss.datachecker.extract.config.ExtractProperties;
import org.opengauss.datachecker.extract.debezium.DataConsolidationService;
import org.opengauss.datachecker.extract.debezium.IncrementDataAnalysisService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.mock.web.MockHttpServletResponse;
import org.springframework.test.context.junit.jupiter.SpringExtension;
import org.springframework.test.web.servlet.MockMvc;

import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;

@ExtendWith(SpringExtension.class)
@WebMvcTest(IncrementController.class)
class IncrementControllerTest {

    @Autowired
    private MockMvc mockMvc;
    @Autowired
    private IncrementController incrementController;
    @MockBean
    private ExtractProperties mockExtractProperties;
    @MockBean
    private IncrementDataAnalysisService mockIncrementDataAnalysisService;
    @MockBean
    private DataConsolidationService mockDataConsolidationService;

    @DisplayName("start increment monitor success")
    @Test
    void testStartIncrementMonitor_success() throws Exception {
        // Setup
        when(mockExtractProperties.getEndpoint()).thenReturn(Endpoint.SOURCE);

        // Run the test
        final MockHttpServletResponse response =
            mockMvc.perform(post("/start/source/increment/monitor").accept(MediaType.APPLICATION_JSON)).andReturn()
                   .getResponse();

        // Verify the results
        assertThat(response.getStatus()).isEqualTo(HttpStatus.OK.value());
        assertThat(response.getContentAsString())
            .isEqualTo("{\"code\":200,\"message\":\"SUCCESS\",\"data\":null,\"success\":true}");
        verify(mockDataConsolidationService).initIncrementConfig();
        verify(mockIncrementDataAnalysisService).startIncrDataAnalysis();
        final AtomicBoolean is_enabled_increment_service = ReflectUtil
            .getField(IncrementController.class, incrementController, AtomicBoolean.class,
                "IS_ENABLED_INCREMENT_SERVICE");
        assertThat(is_enabled_increment_service.get()).isEqualTo(true);
    }

    @DisplayName("start increment monitor, endpoint sink,do nothing")
    @Test
    void testStartIncrementMonitor_sink() throws Exception {
        // Setup
        when(mockExtractProperties.getEndpoint()).thenReturn(Endpoint.SINK);

        // Run the test
        final MockHttpServletResponse response =
            mockMvc.perform(post("/start/source/increment/monitor").accept(MediaType.APPLICATION_JSON)).andReturn()
                   .getResponse();
        // Verify the results
        assertThat(response.getStatus()).isEqualTo(HttpStatus.OK.value());
        assertThat(response.getContentAsString())
            .isEqualTo("{\"code\":200,\"message\":\"SUCCESS\",\"data\":null,\"success\":true}");
        final AtomicBoolean is_enabled_increment_service = ReflectUtil
            .getField(IncrementController.class, incrementController, AtomicBoolean.class,
                "IS_ENABLED_INCREMENT_SERVICE");
        assertThat(is_enabled_increment_service.get()).isEqualTo(false);
    }
}
