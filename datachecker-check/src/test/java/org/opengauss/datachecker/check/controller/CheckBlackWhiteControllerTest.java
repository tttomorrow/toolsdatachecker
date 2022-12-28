package org.opengauss.datachecker.check.controller;

import com.alibaba.fastjson.JSONObject;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.opengauss.datachecker.check.service.CheckBlackWhiteService;
import org.opengauss.datachecker.common.web.Result;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.mock.web.MockHttpServletResponse;
import org.springframework.test.context.junit.jupiter.SpringExtension;
import org.springframework.test.web.servlet.MockMvc;

import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@ExtendWith(SpringExtension.class)
@WebMvcTest(CheckBlackWhiteController.class)
class CheckBlackWhiteControllerTest {

    @Autowired
    private MockMvc mockMvc;

    @MockBean
    private CheckBlackWhiteService mockCheckBlackWhiteService;

    @Test
    void testAddWhiteList() throws Exception {
        // Setup
        List<String> list = List.of("table");
        // Run the test
        mockMvc.perform(
            post("/add/white/list").content(JSONObject.toJSONString(list)).contentType(MediaType.APPLICATION_JSON)
                                   .accept(MediaType.APPLICATION_JSON)).andExpect(status().isOk());
        // Verify the results
    }

    @Test
    void testUpdateWhiteList() throws Exception {
        List<String> list = List.of("table");
        mockMvc.perform(
            post("/update/white/list").content(JSONObject.toJSONString(list)).contentType(MediaType.APPLICATION_JSON)
                                      .accept(MediaType.APPLICATION_JSON)).andExpect(status().isOk());

    }

    @Test
    void testDeleteWhiteList() throws Exception {
        // Setup
        List<String> list = List.of("table");
        // Run the test
        final MockHttpServletResponse response = mockMvc.perform(
            post("/delete/white/list").content(JSONObject.toJSONString(list)).contentType(MediaType.APPLICATION_JSON)
                                      .accept(MediaType.APPLICATION_JSON)).andReturn().getResponse();

        // Verify the results
        assertThat(response.getStatus()).isEqualTo(HttpStatus.OK.value());
    }

    @Test
    void testQueryWhiteList() throws Exception {
        // Setup
        when(mockCheckBlackWhiteService.queryWhiteList()).thenReturn(List.of("value"));

        // Run the test
        final MockHttpServletResponse response =
            mockMvc.perform(post("/query/white/list").accept(MediaType.APPLICATION_JSON)).andReturn().getResponse();

        // Verify the results
        assertThat(response.getStatus()).isEqualTo(HttpStatus.OK.value());
        assertThat(response.getContentAsString()).isEqualTo(JSONObject.toJSONString(Result.success(List.of("value"))));
    }

    @Test
    void testQueryWhiteList_CheckBlackWhiteServiceReturnsNoItems() throws Exception {
        // Setup
        when(mockCheckBlackWhiteService.queryWhiteList()).thenReturn(Collections.emptyList());

        // Run the test
        final MockHttpServletResponse response =
            mockMvc.perform(post("/query/white/list").accept(MediaType.APPLICATION_JSON)).andReturn().getResponse();

        // Verify the results
        assertThat(response.getStatus()).isEqualTo(HttpStatus.OK.value());
        assertThat(response.getContentAsString()).isEqualTo(JSONObject.toJSONString(Result.success(Collections.emptyList())));
    }

    @Test
    void testAddBlackList() throws Exception {
        // Setup
        List<String> list = List.of("table");
        // Run the test
        final MockHttpServletResponse response = mockMvc.perform(
            post("/add/black/list").content(JSONObject.toJSONString(list)).contentType(MediaType.APPLICATION_JSON)
                                   .accept(MediaType.APPLICATION_JSON)).andReturn().getResponse();

        // Verify the results
        assertThat(response.getStatus()).isEqualTo(HttpStatus.OK.value());
    }

    @Test
    void testUpdateBlackList() throws Exception {
        // Setup
        List<String> list = List.of("table");
        // Run the test
        final MockHttpServletResponse response = mockMvc.perform(
            post("/update/black/list").content(JSONObject.toJSONString(list)).contentType(MediaType.APPLICATION_JSON)
                                      .accept(MediaType.APPLICATION_JSON)).andReturn().getResponse();

        // Verify the results
        assertThat(response.getStatus()).isEqualTo(HttpStatus.OK.value());
    }

    @Test
    void testDeleteBlackList() throws Exception {
        // Setup
        List<String> list = List.of("table");
        // Run the test
        final MockHttpServletResponse response = mockMvc.perform(
            post("/delete/black/list").content(JSONObject.toJSONString(list)).contentType(MediaType.APPLICATION_JSON)
                                      .accept(MediaType.APPLICATION_JSON)).andReturn().getResponse();

        // Verify the results
        assertThat(response.getStatus()).isEqualTo(HttpStatus.OK.value());
    }

    @Test
    void testQueryBlackList() throws Exception {
        // Setup
        when(mockCheckBlackWhiteService.queryBlackList()).thenReturn(List.of("value"));

        // Run the test
        final MockHttpServletResponse response =
            mockMvc.perform(post("/query/black/list").accept(MediaType.APPLICATION_JSON)).andReturn().getResponse();

        // Verify the results
        assertThat(response.getStatus()).isEqualTo(HttpStatus.OK.value());
        assertThat(response.getContentAsString()).isEqualTo(JSONObject.toJSONString(Result.success(List.of("value"))));
    }

    @Test
    void testQueryBlackList_CheckBlackWhiteServiceReturnsNoItems() throws Exception {
        // Setup
        when(mockCheckBlackWhiteService.queryBlackList()).thenReturn(Collections.emptyList());

        // Run the test
        final MockHttpServletResponse response =
            mockMvc.perform(post("/query/black/list").accept(MediaType.APPLICATION_JSON)).andReturn().getResponse();

        // Verify the results
        assertThat(response.getStatus()).isEqualTo(HttpStatus.OK.value());
        assertThat(response.getContentAsString()).isEqualTo(JSONObject.toJSONString(Result.success(List.of())));
    }
}
