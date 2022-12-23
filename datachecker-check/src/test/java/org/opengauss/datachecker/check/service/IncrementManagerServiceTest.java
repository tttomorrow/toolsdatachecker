//package org.opengauss.datachecker.check.service;
//
//import org.junit.jupiter.api.Test;
//import org.junit.jupiter.api.extension.ExtendWith;
//import org.mockito.InjectMocks;
//import org.mockito.Mock;
//import org.mockito.junit.jupiter.MockitoExtension;
//import org.opengauss.datachecker.check.BaseTest;
//import org.opengauss.datachecker.check.modules.check.DataCheckService;
//import org.opengauss.datachecker.common.entry.extract.SourceDataLog;
//import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
//
//import java.util.List;
//
//import static org.mockito.Mockito.when;
//
//class IncrementManagerServiceTest  extends BaseTest {
//
//    @Mock
//    private DataCheckService mockDataCheckService;
//    @Mock
//    private ThreadPoolTaskExecutor mockAsyncCheckExecutor;
//
//    @InjectMocks
//    private IncrementManagerService incrementManagerServiceUnderTest;
//
//    @Test
//    void testNotifySourceIncrementDataLogs() {
//        // Setup
//        final SourceDataLog dataLog = new SourceDataLog();
//        dataLog.setTableName("tableName");
//        dataLog.setBeginOffset(0L);
//        dataLog.setCompositePrimarys(List.of("value"));
//        dataLog.setCompositePrimaryValues(List.of("value"));
//        final List<SourceDataLog> dataLogList = List.of(dataLog);
//
//        // Run the test
//        incrementManagerServiceUnderTest.notifySourceIncrementDataLogs(dataLogList);
//
//        // Verify the results
//    }
//
//    @Test
//    void testStartIncrementDataLogs() {
//        // Setup
//        when(mockDataCheckService.incrementCheckTableData("tableName", "process", new SourceDataLog()))
//            .thenReturn(null);
//
//        // Run the test
//        incrementManagerServiceUnderTest.startIncrementDataLogs();
//
//        // Verify the results
//    }
//
//    @Test
//    void testCheckingIncrementDataLogs() {
//        // Setup
//        when(mockDataCheckService.incrementCheckTableData("tableName", "process", new SourceDataLog()))
//            .thenReturn(null);
//
//        // Run the test
//        incrementManagerServiceUnderTest.checkingIncrementDataLogs();
//
//        // Verify the results
//    }
//}
