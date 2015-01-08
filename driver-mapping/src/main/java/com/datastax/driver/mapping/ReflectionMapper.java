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
package com.datastax.driver.mapping;

import java.beans.IntrospectionException;
import java.beans.PropertyDescriptor;
import java.lang.reflect.Field;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.lang.reflect.Method;
import java.util.*;

import com.datastax.driver.core.*;

/**
 * An {@link EntityMapper} implementation that use reflection to read and write fields
 * of an entity.
 */
class ReflectionMapper<T> extends EntityMapper<T> {

    private static ReflectionFactory factory = new ReflectionFactory();

    private ReflectionMapper(Class<T> entityClass, String keyspace, String table, ConsistencyLevel writeConsistency, ConsistencyLevel readConsistency) {
        super(entityClass, keyspace, table, writeConsistency, readConsistency);
    }

    public static Factory factory() {
        return factory;
    }

    @Override
    public T newEntity() {
        try {
            return entityClass.newInstance();
        } catch (Exception e) {
            throw new RuntimeException("Can't create an instance of " + entityClass.getName());
        }
    }

    private static class LiteralMapper<T> extends ColumnMapper<T> {

        private final Method readMethod;
        private final Method writeMethod;

        private LiteralMapper(Field field, int position, PropertyDescriptor pd) {
            this(field, extractType(field), position, pd);
        }

        private LiteralMapper(Field field, DataType type, int position, PropertyDescriptor pd) {
            super(field, type, position);
            this.readMethod = pd.getReadMethod();
            this.writeMethod = pd.getWriteMethod();
        }

        @Override
        public Object getValue(T entity) {
            try {
                return readMethod.invoke(entity);
            } catch (IllegalArgumentException e) {
                throw new IllegalArgumentException("Could not get field '" + fieldName + "'");
            } catch (Exception e) {
                throw new IllegalStateException("Unable to access getter for '" + fieldName + "' in " + entity.getClass().getName(), e);
            }
        }

        @Override
        public void setValue(Object entity, Object value) {
            try {
                writeMethod.invoke(entity, value);
            } catch (IllegalArgumentException e) {
                throw new IllegalArgumentException("Could not set field '" + fieldName + "' to value '" + value + "'");
            } catch (Exception e) {
                throw new IllegalStateException("Unable to access setter for '" + fieldName + "' in " + entity.getClass().getName(), e);
            }
        }
    }

    private static class EnumMapper<T> extends LiteralMapper<T> {

        private final EnumType enumType;
        private final Map<String, Object> fromString;

        private EnumMapper(Field field, int position, PropertyDescriptor pd, EnumType enumType) {
            super(field, enumType == EnumType.STRING ? DataType.text() : DataType.cint(), position, pd);
            this.enumType = enumType;

            if (enumType == EnumType.STRING) {
                fromString = new HashMap<String, Object>(javaType.getEnumConstants().length);
                for (Object constant : javaType.getEnumConstants())
                    fromString.put(constant.toString().toLowerCase(), constant);

            } else {
                fromString = null;
            }
        }

        @SuppressWarnings("rawtypes")
		@Override
        public Object getValue(T entity) {
            Object value = super.getValue(entity);
            switch (enumType) {
                case STRING:
                    return (value == null) ? null : value.toString();
                case ORDINAL:
                    return (value == null) ? null : ((Enum)value).ordinal();
            }
            throw new AssertionError();
        }

        @Override
        public void setValue(Object entity, Object value) {
            Object converted = null;
            switch (enumType) {
                case STRING:
                    converted = fromString.get(value.toString().toLowerCase());
                    break;
                case ORDINAL:
                    converted = javaType.getEnumConstants()[(Integer)value];
                    break;
            }
            super.setValue(entity, converted);
        }
    }

    private static class UDTColumnMapper<T, U> extends LiteralMapper<T> {
        private final UDTMapper<U> udtMapper;

        private UDTColumnMapper(Field field, int position, PropertyDescriptor pd, UDTMapper<U> udtMapper) {
            super(field, udtMapper.getUserType(), position, pd);
            this.udtMapper = udtMapper;
        }

        @Override
        public Object getValue(T entity) {
            @SuppressWarnings("unchecked")
            U udtEntity = (U) super.getValue(entity);
            return udtEntity == null ? null : udtMapper.toUDT(udtEntity);
        }

        @Override
        public void setValue(Object entity, Object value) {
            assert value instanceof UDTValue;
            UDTValue udtValue = (UDTValue) value;
            assert udtValue.getType().equals(udtMapper.getUserType());

            super.setValue(entity, udtMapper.toEntity((udtValue)));
        }
    }

    private static class NestedUDTMapper<T> extends LiteralMapper<T> {
        private final ExtractedType extractedType;

        public NestedUDTMapper(Field field, int position, PropertyDescriptor pd, ExtractedType extractedType) {
            super(field, extractedType.dataType, position, pd);
            this.extractedType = extractedType;
        }

        @Override
        @SuppressWarnings("unchecked")
        public Object getValue(T entity) {
            Object valueWithEntities = super.getValue(entity);
            return (T)convertEntitiesToUDTs(valueWithEntities, extractedType);
        }

        @Override
        public void setValue(Object entity, Object valueWithUDTValues) {
            super.setValue(entity, convertUDTsToEntities(valueWithUDTValues, extractedType));
        }

        @SuppressWarnings("unchecked")
        private Object convertEntitiesToUDTs(Object value, ExtractedType type) {
            if (!type.containsMappedUDT)
                return value;

            if (type.udtMapper != null)
                return type.udtMapper.toUDT(value);

            if (type.dataType.getName() == DataType.Name.LIST) {
                ExtractedType elementType = type.childTypes.get(0);
                List<Object> result = new ArrayList<Object>();
                for (Object element : (List<Object>)value)
                    result.add(convertEntitiesToUDTs(element, elementType));
                return result;
            }

            if (type.dataType.getName() == DataType.Name.SET) {
                ExtractedType elementType = type.childTypes.get(0);
                Set<Object> result = new LinkedHashSet<Object>();
                for (Object element : (Set<Object>)value)
                    result.add(convertEntitiesToUDTs(element, elementType));
                return result;
            }

            if (type.dataType.getName() == DataType.Name.MAP) {
                ExtractedType keyType = type.childTypes.get(0);
                ExtractedType valueType = type.childTypes.get(1);
                Map<Object, Object> result = new LinkedHashMap<Object, Object>();
                for (Map.Entry<Object, Object> entry : ((Map<Object, Object>)value).entrySet())
                    result.put(
                        convertEntitiesToUDTs(entry.getKey(), keyType),
                        convertEntitiesToUDTs(entry.getValue(), valueType)
                    );
                return result;
            }
            throw new IllegalArgumentException("Error converting " + value);
        }

        @SuppressWarnings("unchecked")
        private Object convertUDTsToEntities(Object value, ExtractedType type) {
            if (!type.containsMappedUDT)
                return value;

            if (type.udtMapper != null)
                return type.udtMapper.toEntity((UDTValue)value);

            if (type.dataType.getName() == DataType.Name.LIST) {
                ExtractedType elementType = type.childTypes.get(0);
                List<Object> result = new ArrayList<Object>();
                for (Object element : (List<Object>)value)
                    result.add(convertUDTsToEntities(element, elementType));
                return result;
            }

            if (type.dataType.getName() == DataType.Name.SET) {
                ExtractedType elementType = type.childTypes.get(0);
                Set<Object> result = new LinkedHashSet<Object>();
                for (Object element : (Set<Object>)value)
                    result.add(convertUDTsToEntities(element, elementType));
                return result;
            }

            if (type.dataType.getName() == DataType.Name.MAP) {
                ExtractedType keyType = type.childTypes.get(0);
                ExtractedType valueType = type.childTypes.get(1);
                Map<Object, Object> result = new LinkedHashMap<Object, Object>();
                for (Map.Entry<Object, Object> entry : ((Map<Object, Object>)value).entrySet())
                    result.put(
                        convertUDTsToEntities(entry.getKey(), keyType),
                        convertUDTsToEntities(entry.getValue(), valueType)
                    );
                return result;
            }
            throw new IllegalArgumentException("Error converting " + value);
        }
    }

    static DataType extractType(Field f) {
        Type type = f.getGenericType();

        if (type instanceof ParameterizedType) {
            ParameterizedType pt = (ParameterizedType)type;
            Type raw = pt.getRawType();
            if (!(raw instanceof Class))
                throw new IllegalArgumentException(String.format("Cannot map class %s for field %s", type, f.getName()));

            Class<?> klass = (Class<?>)raw;
            if (TypeMappings.mapsToList(klass)) {
                return DataType.list(TypeMappings.getSimpleType(ReflectionUtils.getParam(pt, 0, f.getName()), f));
            }
            if (TypeMappings.mapsToSet(klass)) {
                return DataType.set(TypeMappings.getSimpleType(ReflectionUtils.getParam(pt, 0, f.getName()), f));
            }
            if (TypeMappings.mapsToMap(klass)) {
                return DataType.map(TypeMappings.getSimpleType(ReflectionUtils.getParam(pt, 0, f.getName()), f), TypeMappings.getSimpleType(ReflectionUtils.getParam(pt, 1, f.getName()), f));
            }
            throw new IllegalArgumentException(String.format("Cannot map class %s for field %s", type, f.getName()));
        }

        if (!(type instanceof Class))
            throw new IllegalArgumentException(String.format("Cannot map class %s for field %s", type, f.getName()));

        return TypeMappings.getSimpleType((Class<?>)type, f);
    }

    private static class ReflectionFactory implements Factory {

        public <T> EntityMapper<T> create(Class<T> entityClass, String keyspace, String table, ConsistencyLevel writeConsistency, ConsistencyLevel readConsistency) {
            return new ReflectionMapper<T>(entityClass, keyspace, table, writeConsistency, readConsistency);
        }

        @SuppressWarnings({ "unchecked", "rawtypes" })
        public <T> ColumnMapper<T> createColumnMapper(Class<T> entityClass, Field field, int position, MappingManager mappingManager) {
            String fieldName = field.getName();
            try {
                PropertyDescriptor pd = new PropertyDescriptor(fieldName, field.getDeclaringClass());

                if (field.getType().isEnum()) {
                    return new EnumMapper<T>(field, position, pd, AnnotationParser.enumType(field));
                }

                if (TypeMappings.isMappedUDT(field.getType())) {
                    UDTMapper<?> udtMapper = mappingManager.getUDTMapper(field.getType());
                    return (ColumnMapper<T>) new UDTColumnMapper(field, position, pd, udtMapper);
                }

                if (field.getGenericType() instanceof ParameterizedType) {
                    ExtractedType extractedType = new ExtractedType(field.getGenericType(), field, mappingManager);
                    if (extractedType.containsMappedUDT) {
                        // We need a specialized mapper to convert UDT instances in the hierarchy.
                        return (ColumnMapper<T>)new NestedUDTMapper(field, position, pd, extractedType);
                    } else {
                        // The default codecs will know how to handle the extracted datatype.
                        return new LiteralMapper<T>(field, extractedType.dataType, position, pd);
                    }
                }

                return new LiteralMapper<T>(field, position, pd);

            } catch (IntrospectionException e) {
                throw new IllegalArgumentException("Cannot find matching getter and setter for field '" + fieldName + "'");
            }
        }
    }
}
