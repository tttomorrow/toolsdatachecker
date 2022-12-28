package org.opengauss.datachecker.extract.debe;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.opengauss.datachecker.extract.util.TestJsonUtil;

import static org.assertj.core.api.Assertions.assertThat;

class DebeziumConsumerListenerTest {

    private DebeziumConsumerListener debeziumConsumerListenerUnderTest;

    @BeforeEach
    void setUp() {
        debeziumConsumerListenerUnderTest = new DebeziumConsumerListener();
    }

    @DisplayName("listen record value empty")
    @Test
    void test_listen_record_value_empty() {
        // Setup
        final ConsumerRecord<String, String> record = new ConsumerRecord<>("topic", 0, 0L, "key", "");
        // Run the test
        debeziumConsumerListenerUnderTest.listen(record);
        // Verify the results
    }

    @DisplayName("listen record value parse error")
    @Test
    void test_listen_record_value_parse_error() {
        // Setup
        final ConsumerRecord<String, String> record = new ConsumerRecord<>("topic", 0, 0L, "key", "value");
        // Run the test
        debeziumConsumerListenerUnderTest.listen(record);
        // Verify the results
    }

    @DisplayName("listen record value parse success")
    @Test
    void test_listen_record_value_parse_success() {
        // Setup
        String value = TestJsonUtil.getJsonText(TestJsonUtil.KEY_DEBEZIUM_ONE_TABLE_RECORD);
        final ConsumerRecord<String, String> record = new ConsumerRecord<>("topic", 0, 0L, "key", value);
        // Run the test
        debeziumConsumerListenerUnderTest.listen(record);
        // Verify the results
    }

    @DisplayName("listen record value parse empty ")
    @Test
    void test_listen_record_value_parse_success_empty_size() {
        assertThat(debeziumConsumerListenerUnderTest.size()).isEqualTo(0);
    }

    @DisplayName("listen record value parse success ,poll")
    @Test
    void test_listen_record_value_parse_success_poll() {
        String value = TestJsonUtil.getJsonText(TestJsonUtil.KEY_DEBEZIUM_ONE_TABLE_RECORD);
        final ConsumerRecord<String, String> record = new ConsumerRecord<>("topic", 0, 0L, "key", value);
        // Run the test
        debeziumConsumerListenerUnderTest.listen(record);
        final DebeziumDataBean result = debeziumConsumerListenerUnderTest.poll();
        assertThat(result.getTable()).isEqualTo("t_test_1_30000_1");
        assertThat(result.getOffset()).isEqualTo(0);
        assertThat(result.getData().get("id")).isEqualTo("29836");
        assertThat(result.getData().get("sizex")).isEqualTo("2251");
        assertThat(result.getData().get("sizey")).isEqualTo("777");
        assertThat(result.getData().get("width")).isEqualTo("403");
        assertThat(result.getData().get("height")).isEqualTo("2492");
        assertThat(result.getData().get("last_upd_user")).isEqualTo("auto_user");
        assertThat(result.getData().get("last_upd_time")).isEqualTo("1671132074000");
        assertThat(result.getData().get("func_id")).isEqualTo(":Ensures that all data changes ar");
        assertThat(result.getData().get("portal_id")).isEqualTo("nge data capture (CDC). Unlike other approac");
        assertThat(result.getData().get("show_order")).isEqualTo("136");
    }
}
