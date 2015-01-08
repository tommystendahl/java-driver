package com.datastax.driver.mapping;

import java.util.Collection;
import java.util.Map;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import org.testng.annotations.Test;

import static com.datastax.driver.core.Assertions.assertThat;

import com.datastax.driver.core.CCMBridge;
import com.datastax.driver.mapping.annotations.Frozen;
import com.datastax.driver.mapping.annotations.PartitionKey;
import com.datastax.driver.mapping.annotations.Table;

public class MapperNestedCollectionsTest extends CCMBridge.PerClassSingleNodeCluster {

    @Override
    protected Collection<String> getTableDefinitions() {
        return Lists.newArrayList("CREATE TABLE foo (k int primary key, m map<text, frozen<map<int, int>>>)");
    }

    @Table(keyspace = "ks", name = "foo")
    public static class Foo {
        @PartitionKey
        private int k;

        @Frozen("map<text, frozen<map<int, int>>>")
        private Map<String, Map<Integer, Integer>> m;

        public int getK() {
            return k;
        }

        public void setK(int k) {
            this.k = k;
        }

        public Map<String, Map<Integer, Integer>> getM() {
            return m;
        }

        public void setM(Map<String, Map<Integer, Integer>> m) {
            this.m = m;
        }
    }

    @Test(groups = "short")
    public void should_save_and_retrieve_entity() {
        Mapper<Foo> mapper = new MappingManager(session).mapper(Foo.class);

        Foo foo = new Foo();
        foo.setK(1);
        foo.setM(ImmutableMap.<String, Map<Integer, Integer>>of("bar", ImmutableMap.of(1, 2)));

        mapper.save(foo);

        Foo foo2 = mapper.get(1);
        assertThat(foo2.getM()).isEqualTo(foo.getM());
    }
}
