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

package org.apache.flink.connector.kafka.dynamic.source.enumerator;

import org.apache.flink.connector.kafka.dynamic.source.split.DynamicKafkaSourceSplit;
import org.apache.flink.connector.kafka.source.split.KafkaPartitionSplit;

import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.NavigableMap;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link ReaderRecoveryCoordinator}. */
class ReaderRecoveryCoordinatorTest {

    @Test
    void testFreshStartHasNoPendingRecovery() {
        ReaderRecoveryCoordinator coordinator = new ReaderRecoveryCoordinator(false);

        assertThat(coordinator.hasPendingRecovery()).isFalse();
        assertThat(coordinator.shouldDeferMetadataUpdateEvents(false)).isFalse();
        assertThat(coordinator.shouldDeferMetadataUpdateEvents(true)).isFalse();
        assertThat(coordinator.hasReportedSplits()).isFalse();
    }

    @Test
    void testRestoredStartGatesUntilInitialRegistrationCompletes() {
        ReaderRecoveryCoordinator coordinator = new ReaderRecoveryCoordinator(true);

        assertThat(coordinator.hasPendingRecovery()).isTrue();
        assertThat(coordinator.shouldDeferMetadataUpdateEvents(true)).isTrue();
        assertThat(coordinator.shouldDeferMetadataUpdateEvents(false)).isTrue();

        coordinator.markInitialRegistrationComplete();

        assertThat(coordinator.hasPendingRecovery()).isFalse();
        assertThat(coordinator.shouldDeferMetadataUpdateEvents(true)).isFalse();
    }

    @Test
    void testReportedSplitsGateUntilAllReadersRegistered() {
        ReaderRecoveryCoordinator coordinator = new ReaderRecoveryCoordinator(false);
        coordinator.recordReportedSplits(1, Collections.singletonList(split("topic", 0)));

        assertThat(coordinator.hasPendingRecovery()).isTrue();
        assertThat(coordinator.hasReportedSplits()).isTrue();
        assertThat(coordinator.shouldDeferMetadataUpdateEvents(false)).isTrue();
        assertThat(coordinator.shouldDeferMetadataUpdateEvents(true)).isFalse();
    }

    @Test
    void testEmptyReportedSplitsAreIgnored() {
        ReaderRecoveryCoordinator coordinator = new ReaderRecoveryCoordinator(false);
        coordinator.recordReportedSplits(1, Collections.emptyList());

        assertThat(coordinator.hasPendingRecovery()).isFalse();
        assertThat(coordinator.hasReportedSplits()).isFalse();
    }

    @Test
    void testDrainReportedSplitsReturnsReaderOrderAndClears() {
        ReaderRecoveryCoordinator coordinator = new ReaderRecoveryCoordinator(false);
        DynamicKafkaSourceSplit splitReader2 = split("topic", 2);
        DynamicKafkaSourceSplit splitReader0 = split("topic", 0);
        coordinator.recordReportedSplits(2, Collections.singletonList(splitReader2));
        coordinator.recordReportedSplits(0, Collections.singletonList(splitReader0));

        NavigableMap<Integer, List<DynamicKafkaSourceSplit>> drained =
                coordinator.drainReportedSplits();

        assertThat(drained.keySet()).containsExactly(0, 2);
        assertThat(drained.get(0)).containsExactly(splitReader0);
        assertThat(drained.get(2)).containsExactly(splitReader2);
        assertThat(coordinator.hasReportedSplits()).isFalse();
        assertThat(coordinator.drainReportedSplits()).isEmpty();
    }

    @Test
    void testDrainDeferredMetadataUpdateReadersReturnsSortedAndClears() {
        ReaderRecoveryCoordinator coordinator = new ReaderRecoveryCoordinator(true);
        coordinator.deferMetadataUpdate(3);
        coordinator.deferMetadataUpdates(Arrays.asList(1, 2, 3));

        assertThat(coordinator.drainDeferredMetadataUpdateReaders()).containsExactly(1, 2, 3);
        assertThat(coordinator.drainDeferredMetadataUpdateReaders()).isEmpty();
    }

    private static DynamicKafkaSourceSplit split(String topic, int partition) {
        return new DynamicKafkaSourceSplit(
                "cluster0", new KafkaPartitionSplit(new TopicPartition(topic, partition), 0L));
    }
}
