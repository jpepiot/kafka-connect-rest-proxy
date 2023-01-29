package com.athena.kafka.connect.restproxy.common;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class VersionUtilsTest {

    @Test
    void givenFileWithVersion_thenFileVersion() {
        assertThat(VersionUtils.getVersion()).isNotNull();
    }
}
