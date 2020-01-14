/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.avro;

import com.google.common.collect.ImmutableMap;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.exception.HoodieException;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TextConverter {


    private static final Map<Schema.Type, TextToAvroFieldProcessor> FIELD_TYPE_PROCESSORS = getFieldTypeProcessors();


    public TextConverter() {
    }


    public GenericRecord convert(String text, Schema schema) {
        return convertTextToAvro(text, schema);
    }

    private static TextToAvroFieldProcessor generateMapTypeHandler() {
        return new TextToAvroFieldProcessor() {
            @Override
            public Pair<Boolean, Object> convert(Object value, String name, Schema schema)
                    throws HoodieTextToAvroConversionException {
                Schema valueSchema = schema.getValueType();
                Map<String, Object> mapRes = new HashMap<String, Object>();
                for (Map.Entry<String, Object> v : ((Map<String, Object>) value).entrySet()) {
                    mapRes.put(v.getKey(), convertJsonToAvroField(v.getValue(), name, valueSchema));
                }
                return Pair.of(true, mapRes);
            }
        };
    }


    private static Map<Schema.Type, TextToAvroFieldProcessor> getFieldTypeProcessors() {
        Map<Schema.Type, TextToAvroFieldProcessor> processorMap =
                new ImmutableMap.Builder<Schema.Type, TextToAvroFieldProcessor>().put(Schema.Type.STRING, generateStringTypeHandler())
                        .put(Schema.Type.BOOLEAN, generateBooleanTypeHandler()).put(Schema.Type.DOUBLE, generateDoubleTypeHandler())
                        .put(Schema.Type.FLOAT, generateFloatTypeHandler()).put(Schema.Type.INT, generateIntTypeHandler())
                        .put(Schema.Type.LONG, generateLongTypeHandler()).put(Schema.Type.ARRAY, generateArrayTypeHandler())
                        .put(Schema.Type.RECORD, generateRecordTypeHandler()).put(Schema.Type.ENUM, generateEnumTypeHandler())
                        .put(Schema.Type.MAP, generateMapTypeHandler()).put(Schema.Type.BYTES, generateBytesTypeHandler())
                        .put(Schema.Type.FIXED, generateFixedTypeHandler()).build();
        return processorMap;
    }


    private static TextToAvroFieldProcessor generateBooleanTypeHandler() {
        return new TextToAvroFieldProcessor() {
            @Override
            public Pair<Boolean, Object> convert(Object value, String name, Schema schema)
                    throws HoodieTextToAvroConversionException {
                if (value instanceof Boolean) {
                    return Pair.of(true, value);
                }
                return Pair.of(false, null);
            }
        };
    }

    private static TextToAvroFieldProcessor generateIntTypeHandler() {
        return new TextToAvroFieldProcessor() {
            @Override
            public Pair<Boolean, Object> convert(Object value, String name, Schema schema)
                    throws HoodieTextToAvroConversionException {
                if (value instanceof Number) {
                    return Pair.of(true, ((Number) value).intValue());
                } else if (value instanceof String) {
                    return Pair.of(true, Integer.valueOf((String) value));
                }
                return Pair.of(false, null);
            }
        };
    }

    private static TextToAvroFieldProcessor generateDoubleTypeHandler() {
        return new TextToAvroFieldProcessor() {
            @Override
            public Pair<Boolean, Object> convert(Object value, String name, Schema schema)
                    throws HoodieTextToAvroConversionException {
                if (value instanceof Number) {
                    return Pair.of(true, ((Number) value).doubleValue());
                } else if (value instanceof String) {
                    return Pair.of(true, Double.valueOf((String) value));
                }
                return Pair.of(false, null);
            }
        };
    }

    private static TextToAvroFieldProcessor generateFloatTypeHandler() {
        return new TextToAvroFieldProcessor() {
            @Override
            public Pair<Boolean, Object> convert(Object value, String name, Schema schema)
                    throws HoodieTextToAvroConversionException {
                if (value instanceof Number) {
                    return Pair.of(true, ((Number) value).floatValue());
                } else if (value instanceof String) {
                    return Pair.of(true, Float.valueOf((String) value));
                }
                return Pair.of(false, null);
            }
        };
    }

    private static TextToAvroFieldProcessor generateLongTypeHandler() {
        return new TextToAvroFieldProcessor() {
            @Override
            public Pair<Boolean, Object> convert(Object value, String name, Schema schema)
                    throws HoodieTextToAvroConversionException {
                if (value instanceof Number) {
                    return Pair.of(true, ((Number) value).longValue());
                } else if (value instanceof String) {
                    return Pair.of(true, Long.valueOf((String) value));
                }
                return Pair.of(false, null);
            }
        };
    }

    private static TextToAvroFieldProcessor generateStringTypeHandler() {
        return new TextToAvroFieldProcessor() {
            @Override
            public Pair<Boolean, Object> convert(Object value, String name, Schema schema)
                    throws HoodieTextToAvroConversionException {
                return Pair.of(true, value.toString());
            }
        };
    }

    private static TextToAvroFieldProcessor generateBytesTypeHandler() {
        return new TextToAvroFieldProcessor() {
            @Override
            public Pair<Boolean, Object> convert(Object value, String name, Schema schema)
                    throws HoodieTextToAvroConversionException {
                return Pair.of(true, value.toString().getBytes());
            }
        };
    }

    private static TextToAvroFieldProcessor generateFixedTypeHandler() {
        return new TextToAvroFieldProcessor() {
            @Override
            public Pair<Boolean, Object> convert(Object value, String name, Schema schema)
                    throws HoodieTextToAvroConversionException {
                byte[] src = value.toString().getBytes();
                byte[] dst = new byte[schema.getFixedSize()];
                System.arraycopy(src, 0, dst, 0, Math.min(schema.getFixedSize(), src.length));
                return Pair.of(true, dst);
            }
        };
    }

    private static TextToAvroFieldProcessor generateEnumTypeHandler() {
        return new TextToAvroFieldProcessor() {
            @Override
            public Pair<Boolean, Object> convert(Object value, String name, Schema schema)
                    throws HoodieTextToAvroConversionException {
                if (schema.getEnumSymbols().contains(value.toString())) {
                    return Pair.of(true, new GenericData.EnumSymbol(schema, value.toString()));
                }
                throw new HoodieTextToAvroConversionException(String.format("Symbol %s not in enum", value.toString()),
                        schema.getFullName(), schema);
            }
        };
    }

    private static TextToAvroFieldProcessor generateRecordTypeHandler() {
        return new TextToAvroFieldProcessor() {
            @Override
            public Pair<Boolean, Object> convert(Object value, String name, Schema schema)
                    throws HoodieTextToAvroConversionException {
                GenericRecord result = new GenericData.Record(schema);
                //TODO
                return Pair.of(true, convertTextToAvro(String.valueOf(value), schema));
            }
        };
    }

    private static TextToAvroFieldProcessor generateArrayTypeHandler() {
        return new TextToAvroFieldProcessor() {
            @Override
            public Pair<Boolean, Object> convert(Object value, String name, Schema schema)
                    throws HoodieTextToAvroConversionException {
                Schema elementSchema = schema.getElementType();
                List listRes = new ArrayList();
                for (Object v : (List) value) {
                    listRes.add(convertJsonToAvroField(v, name, elementSchema));
                }
                return Pair.of(true, listRes);
            }
        };
    }


    private static GenericRecord convertTextToAvro(String text, Schema schema) {
        GenericRecord avroRecord = new GenericData.Record(schema);
        if (!StringUtils.isNullOrEmpty(text)) {
            String[] values = text.split("\t", -1);
            if (values.length == schema.getFields().size()) {
                for (int i = 0; i < schema.getFields().size(); i++) {
                    Schema.Field field = schema.getFields().get(i);
                    avroRecord.put(field.pos(), convertJsonToAvroField(values[0], field.name(), field.schema()));
                }
            }
        }
        return avroRecord;
    }

    private static Object convertJsonToAvroField(Object value, String name, Schema schema) {

        if (isOptional(schema)) {
            if (value == null) {
                return null;
            } else {
                schema = getNonNull(schema);
            }
        } else if (value == null) {
            // Always fail on null for non-nullable schemas
            throw new HoodieTextToAvroConversionException(null, name, schema);
        }

        TextToAvroFieldProcessor processor = FIELD_TYPE_PROCESSORS.get(schema.getType());
        if (null != processor) {
            return processor.convertToAvro(value, name, schema);
        }
        throw new IllegalArgumentException("JsonConverter cannot handle type: " + schema.getType());
    }

    private static Schema getNonNull(Schema schema) {
        List<Schema> types = schema.getTypes();
        Schema.Type firstType = types.get(0).getType();
        return firstType.equals(Schema.Type.NULL) ? types.get(1) : types.get(0);
    }

    private static boolean isOptional(Schema schema) {
        return schema.getType().equals(Schema.Type.UNION) && schema.getTypes().size() == 2
                && (schema.getTypes().get(0).getType().equals(Schema.Type.NULL)
                || schema.getTypes().get(1).getType().equals(Schema.Type.NULL));
    }

    private abstract static class TextToAvroFieldProcessor implements Serializable {

        public Object convertToAvro(Object value, String name, Schema schema) {
            Pair<Boolean, Object> res = convert(value, name, schema);
            if (!res.getLeft()) {
                throw new HoodieTextToAvroConversionException(value, name, schema);
            }
            return res.getRight();
        }

        protected abstract Pair<Boolean, Object> convert(Object value, String name, Schema schema)
                throws HoodieTextToAvroConversionException;
    }

    public static class HoodieTextToAvroConversionException extends HoodieException {
        private Object value;
        private String fieldName;
        private Schema schema;

        public HoodieTextToAvroConversionException(Object value, String fieldName, Schema schema) {
            this.value = value;
            this.fieldName = fieldName;
            this.schema = schema;
        }

        @Override
        public String toString() {
            return String.format("Json to Avro Type conversion error for field %s, %s for %s", fieldName, value, schema);
        }
    }
}