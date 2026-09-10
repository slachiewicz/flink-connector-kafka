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

package org.apache.flink.streaming.connectors.kafka.internals.metrics;

import org.apache.flink.connector.kafka.testutils.TestKafkaContainer;
import org.apache.flink.metrics.Gauge;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.Network;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Properties;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Stream;

import static org.apache.flink.connector.kafka.testutils.KafkaUtil.createKafkaContainer;
import static org.assertj.core.api.Assertions.assertThat;

@Testcontainers
class KafkaMetricMutableWrapperTest {

    private static final String INTER_CONTAINER_KAFKA_ALIAS = "kafka";
    private static final Network NETWORK = Network.newNetwork();

    @Container
    public static final TestKafkaContainer KAFKA_CONTAINER =
            createKafkaContainer(KafkaMetricMutableWrapperTest.class)
                    .withNetwork(NETWORK)
                    .withNetworkAliases(INTER_CONTAINER_KAFKA_ALIAS);

    @Test
    void testOnlyMeasurableMetricsAreRegisteredWithMutableWrapper() {
        testOnlyMeasurableMetricsAreRegistered(KafkaMetricMutableWrapper::new);
    }

    @Test
    void testNonDoubleNumericMetricsAreForwarded() {
        // Kafka clients also expose non-Double numeric metrics, e.g. the app-info group's
        // start-time-ms (a Long). These used to be silently reported as 0.0; see FLINK-27493.
        assertThat(new KafkaMetricMutableWrapper(fakeMetric(42L)).getValue()).isEqualTo(42.0);
        assertThat(new KafkaMetricMutableWrapper(fakeMetric(7)).getValue()).isEqualTo(7.0);
        assertThat(new KafkaMetricMutableWrapper(fakeMetric(1.5)).getValue()).isEqualTo(1.5);
    }

    @Test
    void testNonNumericMetricsFallBackToZero() {
        assertThat(new KafkaMetricMutableWrapper(fakeMetric("4.2.0")).getValue()).isEqualTo(0.0);
    }

    private static Metric fakeMetric(Object value) {
        return new Metric() {
            @Override
            public MetricName metricName() {
                return new MetricName(
                        "test-metric", "test-group", "", java.util.Collections.emptyMap());
            }

            @Override
            public Object metricValue() {
                return value;
            }
        };
    }

    private static void testOnlyMeasurableMetricsAreRegistered(
            Function<Metric, Gauge<Double>> wrapperFactory) {
        final Collection<Gauge<Double>> metricWrappers = new ArrayList<>();
        try (final KafkaConsumer<?, ?> consumer =
                        new KafkaConsumer<>(getKafkaClientConfiguration());
                final KafkaProducer<?, ?> producer =
                        new KafkaProducer<>(getKafkaClientConfiguration())) {
            Stream.concat(
                            consumer.metrics().values().stream(),
                            producer.metrics().values().stream())
                    .map(wrapperFactory::apply)
                    .forEach(metricWrappers::add);

            // Ensure that all values are accessible and return valid double values
            metricWrappers.forEach(Gauge::getValue);
        }
    }

    private static Properties getKafkaClientConfiguration() {
        final Properties standardProps = new Properties();
        standardProps.put("bootstrap.servers", KAFKA_CONTAINER.getBootstrapServers());
        standardProps.put("group.id", UUID.randomUUID().toString());
        standardProps.put("enable.auto.commit", false);
        standardProps.put("key.deserializer", ByteArrayDeserializer.class.getName());
        standardProps.put("value.deserializer", ByteArrayDeserializer.class.getName());
        standardProps.put("key.serializer", ByteArraySerializer.class.getName());
        standardProps.put("value.serializer", ByteArraySerializer.class.getName());
        standardProps.put("auto.offset.reset", "earliest");
        standardProps.put("max.partition.fetch.bytes", 256);
        return standardProps;
    }
}
