package org.opengauss.datachecker.check.modules.check;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opengauss.datachecker.check.client.ExtractSinkFeignClient;
import org.opengauss.datachecker.check.client.ExtractSourceFeignClient;
import org.opengauss.datachecker.check.client.FeignClientService;
import org.opengauss.datachecker.common.entry.check.IncrementDataCheckParam;
import org.opengauss.datachecker.common.entry.enums.Endpoint;
import org.opengauss.datachecker.common.entry.extract.SourceDataLog;
import org.opengauss.datachecker.common.entry.extract.TableMetadataHash;
import org.opengauss.datachecker.common.web.Result;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.boot.test.mock.mockito.MockBeans;

import java.util.List;

import static org.mockito.BDDMockito.given;

@ExtendWith(MockitoExtension.class)
class IncrementCheckThreadTest {

    @MockBean
    private IncrementDataCheckParam mockCheckParam;
    @MockBean
    private DataCheckRunnableSupport mockSupport;
    @MockBean
    private FeignClientService feignClientService;
    @MockBean
    private ExtractSourceFeignClient extractSourceClient;
    @MockBean
    private ExtractSinkFeignClient extractSinkClient;

    private static final String TABLE_NAME = "table";

    @BeforeEach
    void setUp() {

        given(this.mockCheckParam.getProcess()).willReturn("process");
        given(this.mockCheckParam.getTableName()).willReturn(TABLE_NAME);
        given(this.mockCheckParam.getBucketCapacity()).willReturn(20);
        given(this.mockCheckParam.getErrorRate()).willReturn(20);

        given(this.mockSupport.getFeignClientService()).willReturn(feignClientService);
        given(this.mockSupport.getFeignClientService().getClient(Endpoint.SOURCE)).willReturn(extractSourceClient);
        given(this.mockSupport.getFeignClientService().getClient(Endpoint.SINK)).willReturn(extractSinkClient);

        given(this.mockSupport.getFeignClientService().getClient(Endpoint.SOURCE).queryTableMetadataHash(TABLE_NAME))
            .willReturn(Result.success(new TableMetadataHash().setTableName(TABLE_NAME).setTableHash(10000L)));
        given(this.mockSupport.getFeignClientService().getClient(Endpoint.SINK).queryTableMetadataHash(TABLE_NAME))
            .willReturn(Result.success(new TableMetadataHash().setTableName(TABLE_NAME).setTableHash(10000L)));

    }

    @DisplayName("increment check success")
    @Test
    void testRun() {
        final SourceDataLog sourceDataLog = new SourceDataLog();
        sourceDataLog.setTableName(TABLE_NAME).setBeginOffset(0).setCompositePrimarys(List.of("id"))
                     .setCompositePrimaryValues(List.of("1", "2"));
        given(this.mockCheckParam.getDataLog()).willReturn(sourceDataLog);
        // Setup

        // Run the test
        new IncrementCheckThread(mockCheckParam, mockSupport).start();

        // Verify the results
    }
}
