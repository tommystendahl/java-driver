/*
 *      Copyright (C) 2012-2014 DataStax Inc.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */
package com.datastax.driver.core;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.testng.annotations.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The goal of this test is to cover the serialization and deserialization of datatypes.
 *
 * It creates a table with a column of a given type, inserts a value and then tries to retrieve it.
 * This is repeated with a large number of datatypes.
 */
public class DataTypeIntegrationTest extends CCMBridge.PerClassSingleNodeCluster {

    @Override
    protected Collection<String> getTableDefinitions() {
        // Tables are created on the fly in the test method.
        return Lists.newArrayList();
    }

    @Test(groups = "long")
    public void should_insert_and_retrieve_data() {
        ProtocolVersion protocolVersion = cluster.getConfiguration().getProtocolOptions().getProtocolVersionEnum();

        for (TestTable table : allTables()) {
            session.execute(table.createStatement);
            session.execute(table.insertStatement, table.sampleValue);
            Row row = session.execute(table.selectStatement).one();
            Object queriedValue = table.testColumnType.deserialize(row.getBytesUnsafe("v"), protocolVersion);

            assertThat(queriedValue)
                .as("Test failure on simple statement with table:%n%s;%n" +
                        "insert statement:%n%s;%n",
                    table.createStatement,
                    table.insertStatement)
                .isEqualTo(table.sampleValue);

            session.execute(table.truncateStatement);

            PreparedStatement ps = session.prepare(table.insertStatement);
            session.execute(ps.bind(table.sampleValue));
            row = session.execute(table.selectStatement).one();
            queriedValue = table.testColumnType.deserialize(row.getBytesUnsafe("v"), protocolVersion);

            assertThat(queriedValue)
                .as("Test failure on prepared statement with table:%n%s;%n" +
                        "insert statement:%n%s;%n",
                    table.createStatement,
                    table.insertStatement)
                .isEqualTo(table.sampleValue);

            session.execute(table.dropStatement);
        }
    }

    /**
     * Abstracts information about a table (corresponding to a given column type).
     */
    static class TestTable {
        private static final AtomicInteger counter = new AtomicInteger();
        private String tableName = "date_type_test" + counter.incrementAndGet();

        final DataType testColumnType;
        final Object sampleValue;

        final String createStatement;
        final String insertStatement = String.format("INSERT INTO %s (k, v) VALUES (1, ?)", tableName);
        final String selectStatement = String.format("SELECT v FROM %s WHERE k = 1", tableName);
        final String truncateStatement = String.format("TRUNCATE %s", tableName);
        final String dropStatement = String.format("DROP TABLE %s", tableName);

        TestTable(DataType testColumnType, Object sampleValue) {
            this.testColumnType = testColumnType;
            this.sampleValue = sampleValue;

            this.createStatement = String.format("CREATE TABLE %s (k int PRIMARY KEY, v %s)", tableName, testColumnType);
        }
    }

    private static List<TestTable> allTables() {
        List<TestTable> tables = Lists.newArrayList();

        tables.addAll(tablesWithPrimitives());
        tables.addAll(tablesWithCollectionsOfPrimitives());
        tables.addAll(tablesWithMapsOfPrimitives());
        tables.addAll(tablesWithNestedCollections());

        return ImmutableList.copyOf(tables);
    }

    private static List<TestTable> tablesWithPrimitives() {
        List<TestTable> tables = Lists.newArrayList();
        for (Map.Entry<DataType, Object> entry : PrimitiveTypeSamples.ALL.entrySet())
            tables.add(new TestTable(entry.getKey(), entry.getValue()));
        return tables;
    }

    private static List<TestTable> tablesWithCollectionsOfPrimitives() {
        List<TestTable> tables = Lists.newArrayList();
        for (Map.Entry<DataType, Object> entry : PrimitiveTypeSamples.ALL.entrySet()) {

            DataType elementType = entry.getKey();
            Object elementSample = entry.getValue();

            tables.add(new TestTable(DataType.list(elementType), Lists.newArrayList(elementSample, elementSample)));
            tables.add(new TestTable(DataType.set(elementType), Sets.newHashSet(elementSample)));
        }
        return tables;
    }

    private static List<TestTable> tablesWithMapsOfPrimitives() {
        List<TestTable> tables = Lists.newArrayList();
        for (Map.Entry<DataType, Object> keyEntry : PrimitiveTypeSamples.ALL.entrySet()) {
            DataType keyType = keyEntry.getKey();
            Object keySample = keyEntry.getValue();
            for (Map.Entry<DataType, Object> valueEntry : PrimitiveTypeSamples.ALL.entrySet()) {
                DataType valueType = valueEntry.getKey();
                Object valueSample = valueEntry.getValue();

                tables.add(new TestTable(DataType.map(keyType, valueType),
                    ImmutableMap.builder().put(keySample, valueSample).build()));
            }
        }
        return tables;
    }

    private static Collection<? extends TestTable> tablesWithNestedCollections() {
        List<TestTable> tables = Lists.newArrayList();

        // To avoid combinatorial explosion, only use int as the primitive type, and two levels of nesting.
        // This yields collections like list<frozen<map<int, int>>, map<frozen<set<int>>, frozen<list<int>>>, etc.

        // Types and samples for the inner collections like frozen<list<int>>
        Map<DataType, Object> childCollectionSamples = ImmutableMap.<DataType, Object>builder()
            .put(DataType.frozenList(DataType.cint()), Lists.newArrayList(1, 1))
            .put(DataType.frozenSet(DataType.cint()), Sets.newHashSet(1, 2))
            .put(DataType.frozenMap(DataType.cint(), DataType.cint()), ImmutableMap.<Integer, Integer>builder().put(1, 2).put(3, 4).build())
            .build();

        for (Map.Entry<DataType, Object> entry : childCollectionSamples.entrySet()) {
            DataType elementType = entry.getKey();
            Object elementSample = entry.getValue();

            tables.add(new TestTable(DataType.list(elementType), Lists.newArrayList(elementSample, elementSample)));
            tables.add(new TestTable(DataType.set(elementType), Sets.newHashSet(elementSample)));

            for (Map.Entry<DataType, Object> valueEntry : childCollectionSamples.entrySet()) {
                DataType valueType = valueEntry.getKey();
                Object valueSample = valueEntry.getValue();

                tables.add(new TestTable(DataType.map(elementType, valueType),
                    ImmutableMap.builder().put(elementSample, valueSample).build()));
            }
        }
        return tables;
    }
}
