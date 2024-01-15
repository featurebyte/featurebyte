package com.featurebyte.hive.udf;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.junit.jupiter.api.Test;

public class CountDictTransformsTest {
  private final Map<String, DoubleWritable> countDict;
  private final Map<String, DoubleWritable> countDictOther;
  private final ObjectInspector mapValueOI =
      ObjectInspectorFactory.getStandardMapObjectInspector(
          PrimitiveObjectInspectorFactory.writableStringObjectInspector,
          PrimitiveObjectInspectorFactory.writableDoubleObjectInspector);

  private final ObjectInspector nullValueOI =
      PrimitiveObjectInspectorFactory.writableVoidObjectInspector;
  private final ObjectInspector stringValueOI =
      PrimitiveObjectInspectorFactory.javaStringObjectInspector;
  private final ObjectInspector boolValueOI =
      PrimitiveObjectInspectorFactory.javaBooleanObjectInspector;

  public CountDictTransformsTest() {
    countDict = new HashMap<String, DoubleWritable>();
    countDict.put("apple", new DoubleWritable(200));
    countDict.put("orange", new DoubleWritable(210));
    countDict.put("pear", new DoubleWritable(220));
    countDict.put("grape", new DoubleWritable(230));
    countDict.put("guava", new DoubleWritable(240));
    countDict.put("banana", new DoubleWritable(250));
    countDict.put("strawberry", new DoubleWritable(260));
    countDict.put("dürian", new DoubleWritable(260));
    countDict.put("watermelon", new DoubleWritable(Double.NaN));
    countDict.put("kiwi", null);

    countDictOther = new HashMap<String, DoubleWritable>();
    countDictOther.put("apple", new DoubleWritable(100));
    countDictOther.put("orange", new DoubleWritable(110));
    countDictOther.put("watermelon", new DoubleWritable(Double.NaN));
    countDictOther.put("kiwi", null);
    countDictOther.put("pineapple", new DoubleWritable(0));
  }

  @Test
  public void testCountDictEntropy() throws HiveException {
    CountDictEntropyV2 udf = new CountDictEntropyV2();
    ObjectInspector[] arguments = {mapValueOI};
    udf.initialize(arguments);
    GenericUDF.DeferredObject[] args = {new GenericUDF.DeferredJavaObject(countDict)};
    DoubleWritable output = (DoubleWritable) udf.evaluate(args);
    assertEquals(output.get(), 2.0753058086690364);
  }

  @Test
  public void testCountDictEntropyZeroCounts() throws HiveException {
    CountDictEntropyV2 udf = new CountDictEntropyV2();
    ObjectInspector[] arguments = {mapValueOI};
    udf.initialize(arguments);
    GenericUDF.DeferredObject[] args = {new GenericUDF.DeferredJavaObject(countDictOther)};
    DoubleWritable output = (DoubleWritable) udf.evaluate(args);
    assertEquals(output.get(), 0.6920129648318738);
  }

  @Test
  public void testCountDictMostFrequent() throws HiveException {
    CountDictMostFrequentV1 udf = new CountDictMostFrequentV1();
    ObjectInspector[] arguments = {mapValueOI};
    udf.initialize(arguments);
    GenericUDF.DeferredObject[] args = {new GenericUDF.DeferredJavaObject(countDict)};
    Text output = (Text) udf.evaluate(args);
    assertEquals(output, new Text("dürian"));
  }

  @Test
  public void testCountDictMostFrequentValue() throws HiveException {
    CountDictMostFrequentValueV1 udf = new CountDictMostFrequentValueV1();
    ObjectInspector[] arguments = {mapValueOI};
    udf.initialize(arguments);
    GenericUDF.DeferredObject[] args = {new GenericUDF.DeferredJavaObject(countDict)};
    DoubleWritable output = (DoubleWritable) udf.evaluate(args);
    assertEquals(output.get(), 260);
  }

  @Test
  public void testCountDictLeastFrequent() throws HiveException {
    CountDictLeastFrequentV1 udf = new CountDictLeastFrequentV1();
    ObjectInspector[] arguments = {mapValueOI};
    udf.initialize(arguments);
    GenericUDF.DeferredObject[] args = {new GenericUDF.DeferredJavaObject(countDict)};
    Text output = (Text) udf.evaluate(args);
    assertEquals(output, new Text("apple"));
  }

  @Test
  public void testCountDictNumUnique() throws HiveException {
    CountDictNumUniqueV1 udf = new CountDictNumUniqueV1();
    ObjectInspector[] arguments = {mapValueOI};
    udf.initialize(arguments);
    GenericUDF.DeferredObject[] args = {new GenericUDF.DeferredJavaObject(countDict)};
    IntWritable output = (IntWritable) udf.evaluate(args);
    assertEquals(output.get(), 10);
  }

  @Test
  public void testCountDictNumUniqueNullArg() throws HiveException {
    CountDictNumUniqueV1 udf = new CountDictNumUniqueV1();
    ObjectInspector[] arguments = {nullValueOI};
    udf.initialize(arguments);
    GenericUDF.DeferredObject[] args = {new GenericUDF.DeferredJavaObject(null)};
    IntWritable output = (IntWritable) udf.evaluate(args);
    assertEquals(output.get(), 0);
  }

  @Test
  public void testCountDictDeleteKey() throws HiveException {
    ObjectDeleteV1 udf = new ObjectDeleteV1();
    ObjectInspector[] arguments = {mapValueOI, stringValueOI};
    udf.initialize(arguments);
    GenericUDF.DeferredObject[] args = {
      new GenericUDF.DeferredJavaObject(countDict), new GenericUDF.DeferredJavaObject("watermelon"),
    };
    HashMap<String, DoubleWritable> expected = new HashMap<String, DoubleWritable>(countDict);
    expected.remove("watermelon");
    HashMap<String, DoubleWritable> output = (HashMap<String, DoubleWritable>) udf.evaluate(args);
    assertEquals(expected, output);
  }

  @Test
  public void testCountDictCosineSimilarity() throws HiveException {
    CountDictCosineSimilarityV1 udf = new CountDictCosineSimilarityV1();
    ObjectInspector[] arguments = {mapValueOI, mapValueOI};
    udf.initialize(arguments);
    GenericUDF.DeferredObject[] args = {
      new GenericUDF.DeferredJavaObject(countDict),
      new GenericUDF.DeferredJavaObject(countDictOther),
    };
    DoubleWritable output = (DoubleWritable) udf.evaluate(args);
    assertEquals(0.43672656326636466, output.get());
  }

  @Test
  public void testCountDictRelativeFrequency() throws HiveException {
    CountDictRelativeFrequencyV1 udf = new CountDictRelativeFrequencyV1();
    ObjectInspector[] arguments = {mapValueOI, stringValueOI};
    udf.initialize(arguments);
    GenericUDF.DeferredObject[] args = {
      new GenericUDF.DeferredJavaObject(countDict), new GenericUDF.DeferredJavaObject("apple"),
    };
    DoubleWritable output = (DoubleWritable) udf.evaluate(args);
    assertEquals(0.10695187165775401, output.get());
  }

  @Test
  public void testCountDictRank() throws HiveException {
    CountDictRankV1 udf = new CountDictRankV1();
    ObjectInspector[] arguments = {mapValueOI, stringValueOI, boolValueOI};
    udf.initialize(arguments);
    GenericUDF.DeferredObject[] args = {
      new GenericUDF.DeferredJavaObject(countDict),
      new GenericUDF.DeferredJavaObject("banana"),
      new GenericUDF.DeferredJavaObject(new BooleanWritable(false)),
    };
    DoubleWritable output = (DoubleWritable) udf.evaluate(args);
    assertEquals(6, output.get());
  }
}
