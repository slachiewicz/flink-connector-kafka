/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.connector.kafka.source;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.junit.jupiter.api.Test;

import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link KafkaSourceOptions}. */
class KafkaSourceOptionsTest {

    @Test
    void testRemoveInternalConfigOptionsStripsFlinkOnlyKeys() {
        Properties props = new Properties();
        props.setProperty(KafkaSourceOptions.CLIENT_ID_PREFIX.key(), "my-prefix");
        props.setProperty(KafkaSourceOptions.PARTITION_DISCOVERY_INTERVAL_MS.key(), "10000");
        props.setProperty(KafkaSourceOptions.REGISTER_KAFKA_CONSUMER_METRICS.key(), "false");
        props.setProperty(KafkaSourceOptions.COMMIT_OFFSETS_ON_CHECKPOINT.key(), "false");
        props.setProperty(KafkaSourceOptions.POLL_TIMEOUT_MS.key(), "500");
        props.setProperty(KafkaSourceOptions.TOPIC_INTEGRITY_CHECK_ENABLED.key(), "true");

        KafkaSourceOptions.removeInternalConfigOptions(props);

        assertThat(props.stringPropertyNames()).isEmpty();
    }

    @Test
    void testRemoveInternalConfigOptionsKeepsRealKafkaClientKeys() {
        Properties props = new Properties();
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "my-group");
        props.setProperty(KafkaSourceOptions.PARTITION_DISCOVERY_INTERVAL_MS.key(), "10000");

        KafkaSourceOptions.removeInternalConfigOptions(props);

        assertThat(props.stringPropertyNames())
                .containsExactlyInAnyOrder(
                        ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, ConsumerConfig.GROUP_ID_CONFIG);
    }
}
