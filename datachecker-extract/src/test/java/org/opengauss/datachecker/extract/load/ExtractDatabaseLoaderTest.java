package org.opengauss.datachecker.extract.load;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opengauss.datachecker.common.entry.extract.MetadataLoadProcess;
import org.opengauss.datachecker.extract.service.MetaDataService;

import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class ExtractDatabaseLoaderTest {

    @Mock
    private MetaDataService mockMetaDataService;

    @InjectMocks
    private ExtractDatabaseLoader extractDatabaseLoaderUnderTest;

    @Test
    void testLoad() {
        // Setup
        final ExtractEnvironment extractEnvironment = new ExtractEnvironment();

        // Configure MetaDataService.getMetadataLoadProcess(...).
        final MetadataLoadProcess metadataLoadProcess = new MetadataLoadProcess();
        metadataLoadProcess.setTotal(0);
        metadataLoadProcess.setLoadCount(0);
        when(mockMetaDataService.getMetadataLoadProcess()).thenReturn(metadataLoadProcess);

        // Run the test
        extractDatabaseLoaderUnderTest.load(extractEnvironment);

        // Verify the results
        verify(mockMetaDataService).loadMetaDataOfSchemaCache();
    }
}
