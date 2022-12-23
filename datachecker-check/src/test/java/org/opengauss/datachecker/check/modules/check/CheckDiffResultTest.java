package org.opengauss.datachecker.check.modules.check;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opengauss.datachecker.check.client.FeignClientService;
import org.opengauss.datachecker.check.modules.check.AbstractCheckDiffResultBuilder.CheckDiffResultBuilder;
import org.opengauss.datachecker.common.entry.enums.CheckMode;
import org.opengauss.datachecker.common.entry.enums.Endpoint;

import java.util.List;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.BDDMockito.given;

@ExtendWith(MockitoExtension.class)
class CheckDiffResultTest {
    @Mock
    private FeignClientService feignClient;
    private CheckDiffResultBuilder mockBuilder;

    @BeforeEach
    public void setUp() {
        mockBuilder = (CheckDiffResultBuilder) CheckDiffResultBuilder.builder(feignClient);
    }

    @DisplayName("check result success")
    @Test
    void test_check_success() {
        mockBuilder.process("process").schema("test").table("table").checkMode(CheckMode.FULL)
                   .isTableStructureEquals(true).partitions(0).rowCount(100).topic("topic");
        CheckDiffResult checkDiffResult = mockBuilder.build();
        assertThat(checkDiffResult.getMessage()).isEqualTo("test.table_[0] check success");
        assertThat(checkDiffResult.getResult()).isEqualTo("success");
    }

    @DisplayName("check result delete 1")
    @Test
    void test_check_result_delete() {
        given(feignClient.buildRepairStatementDeleteDml(Endpoint.SOURCE, "test", "table", Set.of("1")))
            .willReturn(List.of("delete from test.table id=1"));

        mockBuilder.process("process").schema("test").table("table").checkMode(CheckMode.FULL)
                   .isTableStructureEquals(true).partitions(0).rowCount(100).topic("topic").keyDeleteSet(Set.of("1"))
                   .keyInsertSet(Set.of()).keyUpdateSet(Set.of());
        CheckDiffResult checkDiffResult = mockBuilder.build();
        assertThat(checkDiffResult.getMessage()).isEqualTo("test.table_[0] check failed( insert=0 update=0 delete=1 )");
        assertThat(checkDiffResult.getRepairDelete()).isEqualTo(List.of("delete from test.table id=1"));
        assertThat(checkDiffResult.getResult()).isEqualTo("failed");
    }

    @DisplayName("check result update 1")
    @Test
    void test_check_result_update() {
        given(feignClient.buildRepairStatementUpdateDml(Endpoint.SOURCE, "test", "table", Set.of("1")))
            .willReturn(List.of("update test.table set name='wang' where id=1"));

        mockBuilder.process("process").schema("test").table("table").checkMode(CheckMode.FULL)
                   .isTableStructureEquals(true).partitions(0).rowCount(100).topic("topic").keyDeleteSet(Set.of())
                   .keyInsertSet(Set.of()).keyUpdateSet(Set.of("1"));
        CheckDiffResult checkDiffResult = mockBuilder.build();
        assertThat(checkDiffResult.getMessage()).isEqualTo("test.table_[0] check failed( insert=0 update=1 delete=0 )");
        assertThat(checkDiffResult.getRepairUpdate())
            .isEqualTo(List.of("update test.table set name='wang' where id=1"));
        assertThat(checkDiffResult.getResult()).isEqualTo("failed");
    }

    @DisplayName("check result insert 1")
    @Test
    void test_check_result_insert() {
        given(feignClient.buildRepairStatementInsertDml(Endpoint.SOURCE, "test", "table", Set.of("1")))
            .willReturn(List.of("insert into test.table values (1,'wang')"));

        mockBuilder.process("process").schema("test").table("table").checkMode(CheckMode.FULL)
                   .isTableStructureEquals(true).partitions(0).rowCount(100).topic("topic").keyDeleteSet(Set.of())
                   .keyInsertSet(Set.of("1")).keyUpdateSet(Set.of());
        CheckDiffResult checkDiffResult = mockBuilder.build();
        assertThat(checkDiffResult.getMessage()).isEqualTo("test.table_[0] check failed( insert=1 update=0 delete=0 )");
        assertThat(checkDiffResult.getRepairInsert()).isEqualTo(List.of("insert into test.table values (1,'wang')"));
        assertThat(checkDiffResult.getResult()).isEqualTo("failed");
    }

    @DisplayName("check result table structure not equals")
    @Test
    void test_check_result_table_structure_not_equals() {
        mockBuilder.process("process").schema("test").table("table").checkMode(CheckMode.FULL)
                   .isTableStructureEquals(false).partitions(0).rowCount(100).topic("topic");
        CheckDiffResult checkDiffResult = mockBuilder.build();
        assertThat(checkDiffResult.getMessage())
            .isEqualTo("table structure is not equals , please check the database sync !");
        assertThat(checkDiffResult.getResult()).isEqualTo("failed");
    }

    @DisplayName("check result table structure not equals, only exist source")
    @Test
    void test_check_result_table_structure_not_equals_only_exist_source() {
        mockBuilder.process("process").schema("test").table("table").checkMode(CheckMode.FULL)
                   .isTableStructureEquals(false).isExistTableMiss(true, Endpoint.SOURCE).partitions(0).rowCount(100)
                   .topic("topic");
        CheckDiffResult checkDiffResult = mockBuilder.build();
        assertThat(checkDiffResult.getMessage()).isEqualTo("table [table] ,  only exist in SourceEndpoint!");
        assertThat(checkDiffResult.getResult()).isEqualTo("failed");
    }

    @DisplayName("check result table structure not equals, only exist sink")
    @Test
    void test_check_result_table_structure_not_equals_only_exist_sink() {
        mockBuilder.process("process").schema("test").table("table").checkMode(CheckMode.FULL)
                   .isTableStructureEquals(false).isExistTableMiss(true, Endpoint.SINK).partitions(0).rowCount(100)
                   .topic("topic");
        CheckDiffResult checkDiffResult = mockBuilder.build();
        assertThat(checkDiffResult.getMessage()).isEqualTo("table [table] ,  only exist in SinkEndpoint!");
        assertThat(checkDiffResult.getResult()).isEqualTo("failed");
    }
}
