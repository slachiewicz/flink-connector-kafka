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

package org.apache.flink.streaming.connectors.kafka.table;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.groups.UnregisteredMetricsGroup;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.types.DeserializationException;
import org.apache.flink.util.Collector;
import org.apache.flink.util.UserCodeClassLoader;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Unit tests for {@link DynamicKafkaDeserializationSchema}. */
public class DynamicKafkaDeserializationSchemaTest {

    @Test
    void testClusterMetadataInjection() throws Exception {
        AtomicBoolean metadataConverterUsed = new AtomicBoolean(false);
        DynamicKafkaDeserializationSchema.MetadataConverter[] metadataConverters =
                new DynamicKafkaDeserializationSchema.MetadataConverter[] {
                    record -> {
                        metadataConverterUsed.set(true);
                        return null;
                    }
                };

        DynamicKafkaDeserializationSchema schema =
                new DynamicKafkaDeserializationSchema(
                        1,
                        null,
                        new int[0],
                        new SingleRowDeserializationSchema(),
                        new int[] {0},
                        true,
                        metadataConverters,
                        TypeInformation.of(RowData.class),
                        false,
                        new boolean[] {true});

        schema.open(new TestInitializationContext());

        String clusterId = "cluster-a";
        schema.setKafkaClusterId(clusterId);

        List<RowData> rows = new ArrayList<>();
        schema.deserialize(
                new ConsumerRecord<>("topic", 0, 0L, null, new byte[] {1}),
                new Collector<RowData>() {
                    @Override
                    public void collect(RowData record) {
                        rows.add(record);
                    }

                    @Override
                    public void close() {}
                });

        assertThat(metadataConverterUsed.get()).isFalse();
        assertThat(rows).hasSize(1);

        GenericRowData row = (GenericRowData) rows.get(0);
        assertThat(row.getField(1)).isEqualTo(StringData.fromString(clusterId));
    }

    @Test
    void testNullKeyInUpsertModeThrowsClearException() throws Exception {
        DynamicKafkaDeserializationSchema schema =
                new DynamicKafkaDeserializationSchema(
                        1,
                        new NullDereferencingDeserializationSchema(),
                        new int[] {0},
                        new SingleRowDeserializationSchema(),
                        new int[] {0},
                        false,
                        new DynamicKafkaDeserializationSchema.MetadataConverter[0],
                        TypeInformation.of(RowData.class),
                        true);

        schema.open(new TestInitializationContext());

        ConsumerRecord<byte[], byte[]> record =
                new ConsumerRecord<>("orders", 3, 42L, null, new byte[] {1});

        assertThatThrownBy(
                        () ->
                                schema.deserialize(
                                        record,
                                        new Collector<RowData>() {
                                            @Override
                                            public void collect(RowData record) {}

                                            @Override
                                            public void close() {}
                                        }))
                .isInstanceOf(DeserializationException.class)
                .hasMessageContaining("orders")
                .hasMessageContaining("partition 3")
                .hasMessageContaining("offset 42");
    }

    private static final class NullDereferencingDeserializationSchema
            implements DeserializationSchema<RowData> {
        private static final long serialVersionUID = 1L;

        @Override
        public RowData deserialize(byte[] message) {
            // Models a format that dereferences its input directly. The shipped json/csv
            // formats instead return null on a null message, which silently drops the record
            // rather than throwing -- either way, the guard in
            // DynamicKafkaDeserializationSchema#deserialize must run first so neither behavior
            // (NPE or silent drop) is ever reached for a keyless record in upsert mode.
            return GenericRowData.of(StringData.fromString("key-" + message.length));
        }

        @Override
        public void deserialize(byte[] message, Collector<RowData> out) {
            out.collect(deserialize(message));
        }

        @Override
        public boolean isEndOfStream(RowData nextElement) {
            return false;
        }

        @Override
        public TypeInformation<RowData> getProducedType() {
            return TypeInformation.of(RowData.class);
        }
    }

    private static final class SingleRowDeserializationSchema
            implements DeserializationSchema<RowData> {
        private static final long serialVersionUID = 1L;

        @Override
        public RowData deserialize(byte[] message) {
            return GenericRowData.of(StringData.fromString("value"));
        }

        @Override
        public void deserialize(byte[] message, Collector<RowData> out) {
            out.collect(deserialize(message));
        }

        @Override
        public boolean isEndOfStream(RowData nextElement) {
            return false;
        }

        @Override
        public TypeInformation<RowData> getProducedType() {
            return TypeInformation.of(RowData.class);
        }
    }

    private static final class TestInitializationContext
            implements DeserializationSchema.InitializationContext {
        @Override
        public MetricGroup getMetricGroup() {
            return UnregisteredMetricsGroup.createSourceReaderMetricGroup();
        }

        @Override
        public UserCodeClassLoader getUserCodeClassLoader() {
            ClassLoader classLoader = DynamicKafkaDeserializationSchemaTest.class.getClassLoader();
            return new UserCodeClassLoader() {
                @Override
                public ClassLoader asClassLoader() {
                    return classLoader;
                }

                @Override
                public void registerReleaseHookIfAbsent(
                        String releaseHookName, Runnable releaseHook) {}
            };
        }
    }
}
