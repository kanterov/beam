package org.apache.beam.sdk.values;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.RowCoderGenerator;
import org.apache.beam.sdk.coders.StructuralByteArray;
import org.apache.beam.sdk.schemas.Schema;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

@Measurement(iterations = 3)
@Warmup(iterations = 2)
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@State(Scope.Benchmark)
public class RowBenchmark {

  public static final Schema schema = Schema.builder()
      .addInt32Field("f0")
      .addDoubleField("f1")
      .addByteArrayField("f2")
      .build();

  public static final Coder<Row> coder = RowCoderGenerator.generate(schema, UUID.randomUUID());

  public static Row generateRowWithGetters(Row row) {
    try {

      ByteArrayOutputStream bos = new ByteArrayOutputStream();
      coder.encode(row, bos);

      return coder.decode(new ByteArrayInputStream(bos.toByteArray()));
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  int something = 0;
  Row rowWithStorage0;
  Row rowWithStorage1;

  Row rowWithGetterEquals0;
  Row rowWithGetterEquals1;

  Row withWithDeepEquals0;
  Row rowWithDeepEquals1;

  @Setup(Level.Iteration)
  public void setup() {
    int i = something++;
    double j = (double) i;
    byte z = (byte) i;

    rowWithStorage0 = new RowWithStorageEquals(schema, Arrays.asList(
        i,
        j,
        new StructuralByteArray(new byte[]{ z, z, z, z })
    ));

    rowWithStorage1 = new RowWithStorageEquals(schema, Arrays.asList(
        i,
        j,
        new StructuralByteArray(new byte[]{ z, z, z, z })
    ));

    i = something++;
    j = (double) i;
    z = (byte) i;

    rowWithGetterEquals0 = generateRowWithGetters(new RowWithStorageEquals(schema, Arrays.asList(
        i,
        j,
        new StructuralByteArray(new byte[]{ z, z, z, z })
    )));

    rowWithGetterEquals1 = generateRowWithGetters(new RowWithStorageEquals(schema, Arrays.asList(
        i,
        j,
        new StructuralByteArray(new byte[]{ z, z, z, z })
    )));

    i = something++;
    j = (double) i;
    z = (byte) i;

    withWithDeepEquals0 = new RowWithDeepEquals(schema, Arrays.asList(
        i,
        j,
        new byte[]{ z, z, z, z }
    ));

    rowWithDeepEquals1 = new RowWithDeepEquals(schema, Arrays.asList(
        i,
        j,
        new byte[]{ z, z, z, z }
    ));
  }

  @TearDown(Level.Iteration)
  public void tearDown(Blackhole bh) {
    int i = something++;
    double j = (double) i;
    byte z = (byte) i;

    rowWithStorage0 = new RowWithStorageEquals(schema, Arrays.asList(
        i,
        j,
        new StructuralByteArray(new byte[]{ z, z, z, z })
    ));

    rowWithStorage1 = new RowWithStorageEquals(schema, Arrays.asList(
        i,
        j,
        new StructuralByteArray(new byte[]{ z, z, z, z })
    ));

    i = something++;
    j = (double) i;
    z = (byte) i;

    rowWithGetterEquals0 = generateRowWithGetters(new RowWithStorageEquals(schema, Arrays.asList(
        i,
        j,
        new StructuralByteArray(new byte[]{ z, z, z, z })
    )));

    rowWithGetterEquals1 = generateRowWithGetters(new RowWithStorageEquals(schema, Arrays.asList(
        i,
        j,
        new StructuralByteArray(new byte[]{ z, z, z, z })
    )));

    i = something++;
    j = (double) i;
    z = (byte) i;

    withWithDeepEquals0 = new RowWithDeepEquals(schema, Arrays.asList(
        i,
        j,
        new byte[]{ z, z, z, z }
    ));

    rowWithDeepEquals1 = new RowWithDeepEquals(schema, Arrays.asList(
        i,
        j,
        new byte[]{ z, z, z, z }
    ));

    bh.consume(rowWithGetterEquals0);
    bh.consume(rowWithGetterEquals1);

    bh.consume(rowWithStorage0);
    bh.consume(rowWithStorage1);

    bh.consume(withWithDeepEquals0);
    bh.consume(rowWithDeepEquals1);
    bh.consume(something);
  }


  //@Benchmark
  public void rowWithGetterEquals(Blackhole bh) {
    bh.consume(rowWithGetterEquals0.equals(rowWithGetterEquals1));
  }

  //@Benchmark
  public void rowWithStorageEquals(Blackhole bh) {
    bh.consume(rowWithStorage0.equals(rowWithStorage1));
  }

  @Benchmark
  public void rowWithDeepEquals(Blackhole bh) {
    bh.consume(withWithDeepEquals0.equals(rowWithDeepEquals1));
  }

  //@Benchmark
  public void compareArrayList1(Blackhole bh) {
    ArrayList<Object> a = new ArrayList<>();
    a.add(1);
    a.add(1.0);
    a.add(true);

    ArrayList<Object> b = new ArrayList<>();
    b.add(1);
    b.add(1.0);
    b.add(true);

    bh.consume(Objects.equals(a, b));
  }


  //@Benchmark
  public void compareArrayList2(Blackhole bh) {
    ArrayList<Object> a = new ArrayList<>();
    a.add(1);
    a.add(1.0);
    a.add(true);

    ArrayList<Object> b = new ArrayList<>();
    b.add(1);
    b.add(1.0);
    b.add(true);

    bh.consume(Arrays.equals(a.toArray(), b.toArray()));
  }

  //@Benchmark
  public void compareArrayList3(Blackhole bh) {
    ArrayList<Object> a = new ArrayList<>();
    a.add(1);
    a.add(1.0);
    a.add(true);

    ArrayList<Object> b = new ArrayList<>();
    b.add(1);
    b.add(1.0);
    b.add(true);

    if (a.size() != b.size()) {
      bh.consume(false);
    } else {
      for (int i = 0; i < a.size(); i++) {
        if (!a.get(i).equals(b.get(i))) {
          bh.consume(false);
          return;
        }
      }

      bh.consume(true);
    }
  }

}
