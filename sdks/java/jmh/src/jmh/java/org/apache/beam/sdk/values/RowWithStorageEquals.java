package org.apache.beam.sdk.values;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import org.apache.beam.sdk.schemas.Schema;

public class RowWithStorageEquals extends RowWithStorage {

  RowWithStorageEquals(final Schema schema, final List<Object> values) {
    super(schema, values);
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
      if (!getValue(i).equals(other.getValue(i))) {
        return false;
      }
    }

    return true;
  }
}
