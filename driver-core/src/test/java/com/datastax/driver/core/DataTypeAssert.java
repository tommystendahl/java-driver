package com.datastax.driver.core;

import com.google.common.reflect.TypeToken;
import org.assertj.core.api.AbstractAssert;

import static com.datastax.driver.core.Assertions.assertThat;

public class DataTypeAssert extends AbstractAssert<DataTypeAssert, DataType> {
    public DataTypeAssert(DataType actual) {
        super(actual, DataTypeAssert.class);
    }

    public DataTypeAssert canBeDeserializedAs(TypeToken typeToken) {
        assertThat(actual.canBeDeserializedAs(typeToken)).isTrue();
        return this;
    }

    public DataTypeAssert cannotBeDeserializedAs(TypeToken typeToken) {
        assertThat(actual.canBeDeserializedAs(typeToken)).isFalse();
        return this;
    }
}
