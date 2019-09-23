package org.janusgraph.diskstorage.lucene;

import java.util.*;

import com.google.common.base.Preconditions;
import org.janusgraph.core.schema.Mapping;
import org.janusgraph.diskstorage.indexing.KeyInformation;
import org.janusgraph.graphdb.database.serialize.AttributeUtil;

class FieldMapping {
    private static final String STRING_SUFFIX = "_____s";
    private final String fieldKey;
    private final Mapping mapping;
    private final Class<?> dataType;

    private FieldMapping(String fieldKey, Mapping mapping, Class<?> dataType) {
        this.fieldKey = fieldKey;
        this.mapping = mapping;
        this.dataType = dataType;
    }

    static FieldMapping createFieldMapping(String indexKey, KeyInformation information) {
        Preconditions.checkNotNull(indexKey);
        return Optional.ofNullable(information)
                       .map(i -> createFieldMapping(indexKey, Mapping.getMapping(i), i.getDataType()))
                       .orElseGet(() -> createFieldMapping(indexKey, null, null));
    }

    private static FieldMapping createFieldMapping(String indexKey, Mapping mapping, Class<?> dataType) {
        if (indexKey.endsWith(STRING_SUFFIX)) {
            return new FieldMapping(indexKey, Mapping.STRING, String.class);
        } else {
            return new FieldMapping(indexKey, mapping, dataType);
        }
    }

    Optional<FieldMapping> getDualMapping() {
        if (AttributeUtil.isString(dataType) && mapping == Mapping.TEXTSTRING) {
            return Optional.of(createFieldMapping(fieldKey + STRING_SUFFIX, null, null));
        }
        return Optional.empty();
    }

    static String getMappedName(String fieldName) {
        return fieldName.endsWith(STRING_SUFFIX) ? fieldName.replaceAll(STRING_SUFFIX, "") : fieldName;
    }

    String getFieldKey() {
        return fieldKey;
    }

    Mapping getMapping() {
        return mapping;
    }

    Class<?> getDataType() {
        return dataType;
    }
}
