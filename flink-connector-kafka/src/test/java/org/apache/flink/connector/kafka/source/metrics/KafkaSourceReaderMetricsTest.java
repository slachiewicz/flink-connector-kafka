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

package org.apache.flink.connector.kafka.source.metrics;

import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.testutils.MetricListener;
import org.apache.flink.runtime.metrics.groups.InternalSourceReaderMetricGroup;

import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.util.Collections;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static org.apache.flink.connector.kafka.source.metrics.KafkaSourceReaderMetrics.PARTITION_GROUP;
import static org.apache.flink.connector.kafka.source.metrics.KafkaSourceReaderMetrics.TOPIC_GROUP;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Unit test for {@link KafkaSourceReaderMetrics}. */
class KafkaSourceReaderMetricsTest {

    private static final TopicPartition FOO_0 = new TopicPartition("foo", 0);
    private static final TopicPartition FOO_1 = new TopicPartition("foo", 1);
    private static final TopicPartition BAR_0 = new TopicPartition("bar", 0);
    private static final TopicPartition BAR_1 = new TopicPartition("bar", 1);

    @Test
    void testCurrentOffsetTracking() {
        MetricListener metricListener = new MetricListener();

        final KafkaSourceReaderMetrics kafkaSourceReaderMetrics =
                new KafkaSourceReaderMetrics(
                        InternalSourceReaderMetricGroup.mock(metricListener.getMetricGroup()));

        kafkaSourceReaderMetrics.registerTopicPartition(FOO_0);
        kafkaSourceReaderMetrics.registerTopicPartition(FOO_1);
        kafkaSourceReaderMetrics.registerTopicPartition(BAR_0);
        kafkaSourceReaderMetrics.registerTopicPartition(BAR_1);

        kafkaSourceReaderMetrics.recordCurrentOffset(FOO_0, 15213L);
        kafkaSourceReaderMetrics.recordCurrentOffset(FOO_1, 18213L);
        kafkaSourceReaderMetrics.recordCurrentOffset(BAR_0, 18613L);
        kafkaSourceReaderMetrics.recordCurrentOffset(BAR_1, 15513L);

        assertCurrentOffset(FOO_0, 15213L, metricListener);
        assertCurrentOffset(FOO_1, 18213L, metricListener);
        assertCurrentOffset(BAR_0, 18613L, metricListener);
        assertCurrentOffset(BAR_1, 15513L, metricListener);
    }

    @Test
    void testCommitOffsetTracking() {
        MetricListener metricListener = new MetricListener();

        final KafkaSourceReaderMetrics kafkaSourceReaderMetrics =
                new KafkaSourceReaderMetrics(
                        InternalSourceReaderMetricGroup.mock(metricListener.getMetricGroup()));

        kafkaSourceReaderMetrics.registerTopicPartition(FOO_0);
        kafkaSourceReaderMetrics.registerTopicPartition(FOO_1);
        kafkaSourceReaderMetrics.registerTopicPartition(BAR_0);
        kafkaSourceReaderMetrics.registerTopicPartition(BAR_1);

        kafkaSourceReaderMetrics.recordCommittedOffset(FOO_0, 15213L);
        kafkaSourceReaderMetrics.recordCommittedOffset(FOO_1, 18213L);
        kafkaSourceReaderMetrics.recordCommittedOffset(BAR_0, 18613L);
        kafkaSourceReaderMetrics.recordCommittedOffset(BAR_1, 15513L);

        assertCommittedOffset(FOO_0, 15213L, metricListener);
        assertCommittedOffset(FOO_1, 18213L, metricListener);
        assertCommittedOffset(BAR_0, 18613L, metricListener);
        assertCommittedOffset(BAR_1, 15513L, metricListener);

        final Optional<Counter> commitsSucceededCounter =
                metricListener.getCounter(
                        KafkaSourceReaderMetrics.KAFKA_SOURCE_READER_METRIC_GROUP,
                        KafkaSourceReaderMetrics.COMMITS_SUCCEEDED_METRIC_COUNTER);
        assertThat(commitsSucceededCounter).isPresent();
        assertThat(commitsSucceededCounter.get().getCount()).isEqualTo(0L);

        kafkaSourceReaderMetrics.recordSucceededCommit();

        assertThat(commitsSucceededCounter.get().getCount()).isEqualTo(1L);
    }

    @Test
    void testRecordsLagTracking() throws Exception {
        // FLINK-11912: the per-partition records-lag gauge should report NaN until the
        // underlying Kafka consumer metric becomes available, then track its value. The real
        // metric is only populated once the KafkaConsumer has polled the partition at least
        // once, which maybeAddRecordsLagMetric(KafkaConsumer, TopicPartition) cannot be driven
        // to in a unit test, so this pokes the same private tracking map via reflection instead.
        MetricListener metricListener = new MetricListener();

        final KafkaSourceReaderMetrics kafkaSourceReaderMetrics =
                new KafkaSourceReaderMetrics(
                        InternalSourceReaderMetricGroup.mock(metricListener.getMetricGroup()));

        kafkaSourceReaderMetrics.registerTopicPartition(FOO_0);
        kafkaSourceReaderMetrics.registerTopicPartition(FOO_1);

        assertThat(getRecordsLag(FOO_0, metricListener)).isNaN();
        assertThat(getRecordsLag(FOO_1, metricListener)).isNaN();

        setRecordsLagMetric(kafkaSourceReaderMetrics, FOO_0, 42.0);
        setRecordsLagMetric(kafkaSourceReaderMetrics, FOO_1, 7.0);

        assertThat(getRecordsLag(FOO_0, metricListener)).isEqualTo(42.0);
        assertThat(getRecordsLag(FOO_1, metricListener)).isEqualTo(7.0);
    }

    @Test
    void testNonTrackingTopicPartition() {
        MetricListener metricListener = new MetricListener();
        final KafkaSourceReaderMetrics kafkaSourceReaderMetrics =
                new KafkaSourceReaderMetrics(
                        InternalSourceReaderMetricGroup.mock(metricListener.getMetricGroup()));
        assertThatThrownBy(() -> kafkaSourceReaderMetrics.recordCurrentOffset(FOO_0, 15213L))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> kafkaSourceReaderMetrics.recordCommittedOffset(FOO_0, 15213L))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void testFailedCommit() {
        MetricListener metricListener = new MetricListener();
        final KafkaSourceReaderMetrics kafkaSourceReaderMetrics =
                new KafkaSourceReaderMetrics(
                        InternalSourceReaderMetricGroup.mock(metricListener.getMetricGroup()));
        kafkaSourceReaderMetrics.recordFailedCommit();
        final Optional<Counter> commitsFailedCounter =
                metricListener.getCounter(
                        KafkaSourceReaderMetrics.KAFKA_SOURCE_READER_METRIC_GROUP,
                        KafkaSourceReaderMetrics.COMMITS_FAILED_METRIC_COUNTER);
        assertThat(commitsFailedCounter).isPresent();
        assertThat(commitsFailedCounter.get().getCount()).isEqualTo(1L);
    }

    // ----------- Assertions --------------

    private void assertCurrentOffset(
            TopicPartition tp, long expectedOffset, MetricListener metricListener) {
        final Optional<Gauge<Long>> currentOffsetGauge =
                metricListener.getGauge(
                        KafkaSourceReaderMetrics.KAFKA_SOURCE_READER_METRIC_GROUP,
                        TOPIC_GROUP,
                        tp.topic(),
                        PARTITION_GROUP,
                        String.valueOf(tp.partition()),
                        KafkaSourceReaderMetrics.CURRENT_OFFSET_METRIC_GAUGE);
        assertThat(currentOffsetGauge).isPresent();
        assertThat((long) currentOffsetGauge.get().getValue()).isEqualTo(expectedOffset);
    }

    private void assertCommittedOffset(
            TopicPartition tp, long expectedOffset, MetricListener metricListener) {
        final Optional<Gauge<Long>> committedOffsetGauge =
                metricListener.getGauge(
                        KafkaSourceReaderMetrics.KAFKA_SOURCE_READER_METRIC_GROUP,
                        TOPIC_GROUP,
                        tp.topic(),
                        PARTITION_GROUP,
                        String.valueOf(tp.partition()),
                        KafkaSourceReaderMetrics.COMMITTED_OFFSET_METRIC_GAUGE);
        assertThat(committedOffsetGauge).isPresent();
        assertThat((long) committedOffsetGauge.get().getValue()).isEqualTo(expectedOffset);
    }

    private double getRecordsLag(TopicPartition tp, MetricListener metricListener) {
        final Optional<Gauge<Double>> recordsLagGauge =
                metricListener.getGauge(
                        KafkaSourceReaderMetrics.KAFKA_SOURCE_READER_METRIC_GROUP,
                        TOPIC_GROUP,
                        tp.topic(),
                        PARTITION_GROUP,
                        String.valueOf(tp.partition()),
                        KafkaSourceReaderMetrics.RECORDS_LAG_METRIC_GAUGE);
        assertThat(recordsLagGauge).isPresent();
        return recordsLagGauge.get().getValue();
    }

    @SuppressWarnings("unchecked")
    private static void setRecordsLagMetric(
            KafkaSourceReaderMetrics kafkaSourceReaderMetrics, TopicPartition tp, double lag)
            throws Exception {
        final Field recordsLagMetricsField =
                KafkaSourceReaderMetrics.class.getDeclaredField("recordsLagMetrics");
        recordsLagMetricsField.setAccessible(true);
        ConcurrentMap<TopicPartition, Metric> recordsLagMetrics =
                (ConcurrentMap<TopicPartition, Metric>)
                        recordsLagMetricsField.get(kafkaSourceReaderMetrics);
        if (recordsLagMetrics == null) {
            recordsLagMetrics = new ConcurrentHashMap<>();
            recordsLagMetricsField.set(kafkaSourceReaderMetrics, recordsLagMetrics);
        }
        recordsLagMetrics.put(tp, fakeMetric(lag));
    }

    private static Metric fakeMetric(double value) {
        return new Metric() {
            @Override
            public MetricName metricName() {
                return new MetricName("test-metric", "test-group", "", Collections.emptyMap());
            }

            @Override
            public Object metricValue() {
                return value;
            }
        };
    }
}
