package org.opengauss.datachecker.common.util;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.opengauss.datachecker.common.entry.enums.DataBaseType;

import static org.assertj.core.api.Assertions.assertThat;

class SqlUtilTest {
    @DisplayName("test mysql escape ")
    @Test
    void testEscape() {
        assertThat(SqlUtil.escape("content", DataBaseType.MS)).isEqualTo("`content`");
    }

    @DisplayName("test opengauss escape ")
    @Test
    void testOpenGaussEscape() {
        assertThat(SqlUtil.escape("content", DataBaseType.OG)).isEqualTo("\"content\"");
    }
}
