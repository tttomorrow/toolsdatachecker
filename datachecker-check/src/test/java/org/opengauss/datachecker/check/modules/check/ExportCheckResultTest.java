package org.opengauss.datachecker.check.modules.check;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.opengauss.datachecker.check.client.FeignClientService;

import static org.assertj.core.api.Assertions.assertThat;

class ExportCheckResultTest {
    private static final String init_path = "path";

    @BeforeEach
    void setUp() {
        ExportCheckResult.initEnvironment(init_path);
    }

    @Test
    void testExport() {
        // Setup
        final CheckDiffResult result =
            new CheckDiffResult(AbstractCheckDiffResultBuilder.builder());
        // Run the test
        ExportCheckResult.export(result);
        // Verify the results
    }

    @Test
    void testBackCheckResultDirectory() {
        // Setup
        // Run the test
        ExportCheckResult.backCheckResultDirectory();
        // Verify the results
    }

    @Test
    void testGetResultPath() {
        assertThat(ExportCheckResult.getResultPath()).isEqualTo(init_path + "\\result\\");
    }
}
