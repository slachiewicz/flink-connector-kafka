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

package org.apache.flink.connector.kafka.source.reader;

import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.source.SourceOutput;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.connector.kafka.source.split.KafkaPartitionSplit;
import org.apache.flink.connector.kafka.source.split.KafkaPartitionSplitState;
import org.apache.flink.util.Collector;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link KafkaRecordEmitter}. */
public class KafkaRecordEmitterTest {

    private static final ConsumerRecord<byte[], byte[]> RECORD =
            new ConsumerRecord<>("topic", 0, 0L, new byte[0], new byte[0]);

    @Test
    public void testDownstreamExceptionIsNotMisreportedAsDeserializationFailure() {
        // Simulates a chained downstream operator throwing during collect(), which is not a
        // deserialization failure and should not be reported as one (FLINK-32303).
        RuntimeException downstreamFailure = new RuntimeException("boom from downstream operator");
        KafkaRecordEmitter<String> emitter =
                new KafkaRecordEmitter<>(new PassThroughDeserializationSchema<>(record -> "value"));
        SourceOutput<String> failingOutput = new ThrowingSourceOutput<>(downstreamFailure);

        assertThatThrownBy(() -> emitter.emitRecord(RECORD, failingOutput, newSplitState()))
                .isSameAs(downstreamFailure)
                .hasMessage("boom from downstream operator");
    }

    @Test
    public void testGenuineDeserializationFailurePropagatesUnwrapped() {
        IOException deserializationFailure = new IOException("could not parse record");
        KafkaRecordEmitter<String> emitter =
                new KafkaRecordEmitter<>(
                        new PassThroughDeserializationSchema<>(
                                record -> {
                                    throw deserializationFailure;
                                }));

        assertThatThrownBy(
                        () -> emitter.emitRecord(RECORD, new NoopSourceOutput<>(), newSplitState()))
                .isSameAs(deserializationFailure);
    }

    @Test
    public void testSuccessfulEmitAdvancesSplitOffset() throws Exception {
        KafkaRecordEmitter<String> emitter =
                new KafkaRecordEmitter<>(new PassThroughDeserializationSchema<>(record -> "value"));
        KafkaPartitionSplitState splitState = newSplitState();

        emitter.emitRecord(RECORD, new NoopSourceOutput<>(), splitState);

        assertThat(splitState.getCurrentOffset()).isEqualTo(RECORD.offset() + 1);
    }

    private static KafkaPartitionSplitState newSplitState() {
        return new KafkaPartitionSplitState(
                new KafkaPartitionSplit(
                        new TopicPartition(RECORD.topic(), RECORD.partition()), 0L));
    }

    /** A minimal deserialization schema that maps a record via a given function. */
    private static class PassThroughDeserializationSchema<T>
            implements KafkaRecordDeserializationSchema<T> {
        private final Deserialize<T> deserialize;

        private PassThroughDeserializationSchema(Deserialize<T> deserialize) {
            this.deserialize = deserialize;
        }

        @Override
        public void deserialize(ConsumerRecord<byte[], byte[]> record, Collector<T> out)
                throws IOException {
            out.collect(deserialize.apply(record));
        }

        @Override
        @SuppressWarnings("unchecked")
        public TypeInformation<T> getProducedType() {
            return (TypeInformation<T>) BasicTypeInfo.STRING_TYPE_INFO;
        }

        private interface Deserialize<T> {
            T apply(ConsumerRecord<byte[], byte[]> record) throws IOException;
        }
    }

    private static class NoopSourceOutput<T> implements SourceOutput<T> {
        @Override
        public void collect(T record) {}

        @Override
        public void collect(T record, long timestamp) {}

        @Override
        public void emitWatermark(Watermark watermark) {}

        @Override
        public void markIdle() {}

        @Override
        public void markActive() {}
    }

    private static class ThrowingSourceOutput<T> implements SourceOutput<T> {
        private final RuntimeException failure;

        private ThrowingSourceOutput(RuntimeException failure) {
            this.failure = failure;
        }

        @Override
        public void collect(T record) {
            throw failure;
        }

        @Override
        public void collect(T record, long timestamp) {
            throw failure;
        }

        @Override
        public void emitWatermark(Watermark watermark) {}

        @Override
        public void markIdle() {}

        @Override
        public void markActive() {}
    }
}
