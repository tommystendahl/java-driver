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

import java.nio.ByteBuffer;
import java.util.*;

import com.google.common.collect.*;
import org.testng.annotations.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The goal of this test is to cover the serialization and deserialization of datatypes.
 *
 * It creates a table with a column of a given type, inserts a value and then tries to retrieve it.
 * This is repeated with a large number of datatypes.
 */
public class DataTypeIntegrationTest extends CCMBridge.PerClassSingleNodeCluster {

    // TODO refactor tests that depend on that and remove
    @Deprecated
    static HashMap<DataType, Object> getSampleData() {
        return null;
    }

    // TODO refactor tests that depend on that and remove
    @Deprecated
    public static Object getCollectionSample(DataType.Name name, DataType dataType) {
        return null;
    }

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
            ByteBuffer rawValue = row.getBytesUnsafe("v");
            Object queriedValue = table.testColumnType.deserialize(rawValue, protocolVersion);

            assertThat(queriedValue)
                .as("Test failure with table:%n%s;%n" +
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
        final DataType testColumnType;
        final Object sampleValue;

        final String createStatement;
        final String insertStatement = "INSERT INTO data_type_test (k, v) VALUES (1, ?)";
        final String selectStatement = "SELECT v FROM data_type_test WHERE k = 1";
        final String dropStatement = "DROP TABLE data_type_test";

        TestTable(DataType testColumnType, Object sampleValue) {
            this.testColumnType = testColumnType;
            this.sampleValue = sampleValue;

            this.createStatement = String.format("CREATE TABLE data_type_test (k int PRIMARY KEY, v %s)", testColumnType);
        }
    }

    private static List<TestTable> allTables() {
        List<TestTable> tables = Lists.newArrayList();

        tables.addAll(tablesWithPrimitives());
        tables.addAll(tablesWithCollectionsOfPrimitives());
        tables.addAll(tablesWithMapsOfPrimitives());

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

                Map<Object, Object> mapSample = Maps.newHashMap();
                mapSample.put(keySample, valueSample);
                tables.add(new TestTable(DataType.map(keyType, valueType), mapSample));
            }
        }
        return tables;
    }
}