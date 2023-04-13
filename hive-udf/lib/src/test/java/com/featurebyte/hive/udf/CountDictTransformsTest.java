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
  }

  @Test
  public void testCountDictEntropy() throws HiveException {
    CountDictEntropy udf = new CountDictEntropy();
    ObjectInspector[] arguments = {mapValueOI};
    udf.initialize(arguments);
    GenericUDF.DeferredObject[] args = {new GenericUDF.DeferredJavaObject(countDict)};
    DoubleWritable output = (DoubleWritable) udf.evaluate(args);
    assertEquals(output.get(), 2.0753058086690364);
  }

  @Test
  public void testCountDictMostFrequent() throws HiveException {
    CountDictMostFrequent udf = new CountDictMostFrequent();
    ObjectInspector[] arguments = {mapValueOI};
    udf.initialize(arguments);
    GenericUDF.DeferredObject[] args = {new GenericUDF.DeferredJavaObject(countDict)};
    Text output = (Text) udf.evaluate(args);
    assertEquals(output, new Text("dürian"));
  }

  @Test
  public void testCountDictMostFrequentValue() throws HiveException {
    CountDictMostFrequentValue udf = new CountDictMostFrequentValue();
    ObjectInspector[] arguments = {mapValueOI};
    udf.initialize(arguments);
    GenericUDF.DeferredObject[] args = {new GenericUDF.DeferredJavaObject(countDict)};
    DoubleWritable output = (DoubleWritable) udf.evaluate(args);
    assertEquals(output.get(), 260);
  }

  @Test
  public void testCountDictNumUnique() throws HiveException {
    CountDictNumUnique udf = new CountDictNumUnique();
    ObjectInspector[] arguments = {mapValueOI};
    udf.initialize(arguments);
    GenericUDF.DeferredObject[] args = {new GenericUDF.DeferredJavaObject(countDict)};
    IntWritable output = (IntWritable) udf.evaluate(args);
    assertEquals(output.get(), 10);
  }

  @Test
  public void testCountDictDeleteKey() throws HiveException {
    ObjectDelete udf = new ObjectDelete();
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
    CountDictCosineSimilarity udf = new CountDictCosineSimilarity();
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
    CountDictRelativeFrequency udf = new CountDictRelativeFrequency();
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
    CountDictRank udf = new CountDictRank();
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
