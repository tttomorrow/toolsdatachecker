package org.opengauss.datachecker.extract.load;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opengauss.datachecker.common.entry.extract.TableMetadata;
import org.opengauss.datachecker.extract.service.MetaDataService;
import org.opengauss.datachecker.extract.util.TestJsonUtil;

import java.util.HashMap;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class ThreadPoolLoaderTest {

    @Mock
    private MetaDataService mockMetaDataService;

    @InjectMocks
    private ThreadPoolLoader threadPoolLoaderUnderTest;

    @Test
    void testLoad() {
        // Setup
        final ExtractEnvironment extractEnvironment = new ExtractEnvironment();
        final HashMap<String, TableMetadata> tableMetadata =
            TestJsonUtil.parseHashMap(TestJsonUtil.KEY_META_DATA_13_TABLE, TableMetadata.class);

        when(mockMetaDataService.queryMetaDataOfSchemaCache()).thenReturn(tableMetadata);
        // Run the test
        threadPoolLoaderUnderTest.load(extractEnvironment);
        // Verify the results
        assertThat(extractEnvironment.getThreadPoolExecutor()).isNotNull();
        extractEnvironment.getThreadPoolExecutor().shutdown();
    }
}
