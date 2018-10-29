package org.apache.beam.sdk.values;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.RowCoderGenerator;
import org.apache.beam.sdk.coders.StructuralByteArray;
import org.apache.beam.sdk.schemas.JavaBeanSchema;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.SchemaRegistry;
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

  private static final Schema pojoCoder =
      new JavaBeanSchema().schemaFor(TypeDescriptor.of(POJO.class));

  public static final Coder<Row> coder = RowCoderGenerator.generate(schema, UUID.randomUUID());

  public static RowWithGetters generateRowWithGetters(POJO pojo) {
    try {
      SchemaRegistry registry = SchemaRegistry.createDefault();
      Row row = registry.getToRowFunction(POJO.class).apply(pojo);

      return (RowWithGetters) row;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  int something = 0;
  RowWithStorage rowWithStorage0;
  RowWithStorage rowWithStorage1;
  RowWithStorage rowWithStorage2;

  RowWithGetters rowWithGetterEquals0;
  RowWithGetters rowWithGetterEquals1;
  RowWithGetters rowWithGetterEquals2;

  RowWithDeepEquals rowWithDeepEquals0;
  RowWithDeepEquals rowWithDeepEquals1;
  RowWithDeepEquals rowWithDeepEquals2;

  public Object[] generateRandomValues() {
    int i = something;
    double j = (double) i;
    byte z = (byte) i;

    return new Object[]{ i, j, new byte[]{ z, z, z, z }};
  }

  public Object[] generateRandomStructuralValues() {
    int i = something;
    double j = (double) i;
    byte z = (byte) i;

    return new Object[]{ i, j, new StructuralByteArray(new byte[]{ z, z, z, z }) };
  }

  @Setup(Level.Trial)
  public void setup() {
    something++;
    rowWithStorage0 = new RowWithStorageEquals(schema, Arrays.asList(generateRandomStructuralValues()));
    rowWithStorage1 = new RowWithStorageEquals(schema, Arrays.asList(generateRandomStructuralValues()));
    something++;
    rowWithStorage2 = new RowWithStorageEquals(schema, Arrays.asList(generateRandomStructuralValues()));

    something++;
    rowWithGetterEquals0 = generateRowWithGetters(POJO.valueOf(generateRandomValues()));
    rowWithGetterEquals1 = generateRowWithGetters(POJO.valueOf(generateRandomValues()));
    something++;
    rowWithGetterEquals2 = generateRowWithGetters(POJO.valueOf(generateRandomValues()));

    something++;
    rowWithDeepEquals0 = new RowWithDeepEquals(schema, Arrays.asList(generateRandomValues()));
    rowWithDeepEquals1 = new RowWithDeepEquals(schema, Arrays.asList(generateRandomValues()));
    something++;
    rowWithDeepEquals2 = new RowWithDeepEquals(schema, Arrays.asList(generateRandomValues()));
  }

  @TearDown(Level.Trial)
  public void tearDown(Blackhole bh) {
    something++;
    rowWithStorage0 = new RowWithStorageEquals(schema, Arrays.asList(generateRandomStructuralValues()));
    rowWithStorage1 = new RowWithStorageEquals(schema, Arrays.asList(generateRandomStructuralValues()));
    something++;
    rowWithStorage2 = new RowWithStorageEquals(schema, Arrays.asList(generateRandomStructuralValues()));

    something++;
    rowWithGetterEquals0 = generateRowWithGetters(POJO.valueOf(generateRandomValues()));
    rowWithGetterEquals1 = generateRowWithGetters(POJO.valueOf(generateRandomValues()));
    something++;
    rowWithGetterEquals2 = generateRowWithGetters(POJO.valueOf(generateRandomValues()));

    something++;
    rowWithDeepEquals0 = new RowWithDeepEquals(schema, Arrays.asList(generateRandomValues()));
    rowWithDeepEquals1 = new RowWithDeepEquals(schema, Arrays.asList(generateRandomValues()));
    something++;
    rowWithDeepEquals2 = new RowWithDeepEquals(schema, Arrays.asList(generateRandomValues()));

    bh.consume(rowWithGetterEquals0);
    bh.consume(rowWithGetterEquals1);
    bh.consume(rowWithGetterEquals2);

    bh.consume(rowWithStorage0);
    bh.consume(rowWithStorage1);
    bh.consume(rowWithStorage2);

    bh.consume(rowWithDeepEquals0);
    bh.consume(rowWithDeepEquals1);
    bh.consume(rowWithDeepEquals2);
    bh.consume(something);
  }

  //@Benchmark
  public void rowWithGetterEquals(Blackhole bh) {
    bh.consume(rowWithGetterEquals0.equals(rowWithGetterEquals1));
    bh.consume(rowWithGetterEquals1.equals(rowWithGetterEquals2));
  }

  //@Benchmark
  public void rowWithStorageEquals(Blackhole bh) {
    bh.consume(rowWithStorage0.equals(rowWithStorage1));
    bh.consume(rowWithStorage1.equals(rowWithStorage2));
  }

  //@Benchmark
  public void rowWithDeepEquals(Blackhole bh) {
    bh.consume(rowWithDeepEquals0.equals(rowWithDeepEquals1));
    bh.consume(rowWithDeepEquals1.equals(rowWithDeepEquals2));
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

  //@Benchmark
  public void testObjectEquals(Blackhole bh) {
    Object[] a = new Object[] { 1, 1.0, new StructuralByteArray(new byte[] { 1, 2, 3, 4 }) };
    Object[] b = new Object[] { 1, 1.0, new StructuralByteArray(new byte[] { 1, 2, 3, 4 }) };

    bh.consume(Arrays.equals(a, b));
  }

  //@Benchmark
  public void testPojoEquals(Blackhole bh) {
    POJO a = new POJO(1, 1.0, new byte[] { 1, 2, 3, 4 });
    POJO b = new POJO(1, 1.0, new byte[] { 1, 2, 3, 4 });

    bh.consume(a.equals(b));
  }

  @Benchmark
  public void testObjectAllocation(Blackhole bh) {
    bh.consume(new Object[] { 1, 1.0, new StructuralByteArray(new byte[] { 1, 2, 3, 4 }) });
  }

  @Benchmark
  public void testPojoAllocation(Blackhole bh) {
    bh.consume(new POJO(1, 1.0, new byte[] { 1, 2, 3, 4 }));
  }
}
