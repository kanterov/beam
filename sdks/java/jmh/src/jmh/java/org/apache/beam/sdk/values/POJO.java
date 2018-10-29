package org.apache.beam.sdk.values;

import java.util.Arrays;
import java.util.Objects;
import org.apache.beam.sdk.schemas.DefaultSchema;
import org.apache.beam.sdk.schemas.JavaBeanSchema;

@DefaultSchema(JavaBeanSchema.class)
public class POJO {
  private int f0;
  private double f1;
  private byte[] f2;

  public POJO(final int f0, final double f1, final byte[] f2) {
    this.f0 = f0;
    this.f1 = f1;
    this.f2 = f2;
  }

  public static POJO valueOf(Object[] values) {
    return new POJO((Integer) values[0], (Double) values[1], (byte[]) values[2]);
  }

  public int getF0() {
    return f0;
  }

  public void setF0(final int f0) {
    this.f0 = f0;
  }

  public double getF1() {
    return f1;
  }

  public void setF1(final double f1) {
    this.f1 = f1;
  }

  public byte[] getF2() {
    return f2;
  }

  public void setF2(final byte[] f2) {
    this.f2 = f2;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    final POJO pojo = (POJO) o;
    return f0 == pojo.f0 &&
           f1 == pojo.f1 &&
           Arrays.equals(f2, pojo.f2);
  }

  @Override
  public int hashCode() {
    int result = Objects.hash(f0, f1);
    result = 31 * result + Arrays.hashCode(f2);
    return result;
  }
}
