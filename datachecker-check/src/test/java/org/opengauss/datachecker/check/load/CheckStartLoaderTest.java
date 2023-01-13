package org.opengauss.datachecker.check.load;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opengauss.datachecker.check.service.CheckService;
import org.opengauss.datachecker.check.service.IncrementManagerService;
import org.opengauss.datachecker.common.entry.enums.CheckMode;

import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class CheckStartLoaderTest {

    @Mock
    private CheckService mockCheckService;
    @Mock
    private IncrementManagerService mockIncrementManagerService;

    @InjectMocks
    private CheckStartLoader checkStartLoaderUnderTest;

    @DisplayName("start full mode")
    @Test
    void testLoad_start_full_mode() {
        // Setup
        final CheckEnvironment checkEnvironment = new CheckEnvironment();
        checkEnvironment.setCheckMode(CheckMode.FULL);
        when(mockCheckService.start(CheckMode.FULL)).thenReturn("result");
        // Run the test
        checkStartLoaderUnderTest.load(checkEnvironment);
        // Verify the results
        verify(mockCheckService).start(CheckMode.FULL);
    }

    @DisplayName("start increment load")
    @Test
    void testLoad() {
        // Setup
        final CheckEnvironment checkEnvironment = new CheckEnvironment();
        checkEnvironment.setCheckMode(CheckMode.INCREMENT);
        doNothing().when(mockIncrementManagerService).startIncrementDataLogs();
        // Run the test
        checkStartLoaderUnderTest.load(checkEnvironment);
    }
}
