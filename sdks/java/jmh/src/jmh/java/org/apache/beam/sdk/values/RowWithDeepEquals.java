package org.apache.beam.sdk.values;

import java.util.Arrays;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Objects;
import org.apache.beam.sdk.schemas.Schema;

public class RowWithDeepEquals extends RowWithStorage {

  RowWithDeepEquals(final Schema schema, final List<Object> values) {
    super(schema, values);
  }

  public static <K, V> boolean mapDeepEquals(
      Map<K, V> a,
      Map<K, V> b,
      Schema.FieldType valueType) {
    if (a == b) {
      return true;
    }

    if (a.size() != b.size()) {
      return false;
    }

    for (Map.Entry<K, V> e : a.entrySet()) {
      K key = e.getKey();
      V value = e.getValue();
      V otherValue = b.get(key);

      if (value == null) {
        if (otherValue != null || !b.containsKey(key)) {
          return false;
        }
      } else {
        if (!deepEquals(value, otherValue, valueType)) {
          return false;
        }
      }
    }

    return true;
  }

  public static boolean listDeepEquals(List<Object> a, List<Object> b,
                                      Schema.FieldType elementType) {
    if (a == b) {
      return true;
    }

    if (a.size() != b.size()) {
      return false;
    }

    for (int i = 0; i < a.size(); i++) {
      if (!deepEquals(a.get(i), b.get(i), elementType)) {
        return false;
      }
    }

    return true;
  }

  public static boolean deepEquals(Object a, Object b, Schema.FieldType fieldType) {
    if (fieldType.getTypeName() == Schema.TypeName.BYTES) {
      return Arrays.equals((byte[])a, (byte[])b);
    } if (fieldType.getTypeName() == Schema.TypeName.ARRAY) {
      return listDeepEquals((List<Object>)a, (List<Object>)b, fieldType.getCollectionElementType());
    } if (fieldType.getTypeName() == Schema.TypeName.MAP) {
      return mapDeepEquals((Map<Object, Object>)a, (Map<Object, Object>)b, fieldType.getMapValueType());
    } else {
      return Objects.equals(a, b);
    }
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof Row)) {
      return false;
    }
    Row other = (Row) o;

    if (!Objects.equals(getSchema(), other.getSchema())) {
      return false;
    }

    for (int i = 0; i < getFieldCount(); i++) {
      if (!deepEquals(getValue(i), other.getValue(i), getSchema().getField(i).getType())) {
        return false;
      }
    }

    return true;
  }
}
