package org.apache.beam.sdk.values;

import java.util.List;
import java.util.Objects;
import org.apache.beam.sdk.schemas.Schema;

public class RowWithCustomDeepEquals extends RowWithStorage {

  RowWithCustomDeepEquals(final Schema schema, final List<Object> values) {
    super(schema, values);
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    Row other = (Row) o;
    return Objects.equals(getSchema(), other.getSchema())
           && Objects.deepEquals(getValues().toArray(), other.getValues().toArray());
  }
}
