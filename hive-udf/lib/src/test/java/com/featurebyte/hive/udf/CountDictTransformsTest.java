package com.featurebyte.hive.udf;

import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.ObjectWritable;
import org.apache.hadoop.io.Text;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class CountDictTransformsTest {
  final private Map<String, IntWritable> countDict;
  final private ObjectInspector mapValueOI = ObjectInspectorFactory.getStandardMapObjectInspector(
    PrimitiveObjectInspectorFactory.writableStringObjectInspector,
    PrimitiveObjectInspectorFactory.writableIntObjectInspector
  );

  public CountDictTransformsTest() {
    countDict = new HashMap<String, IntWritable>();
    countDict.put("apple", new IntWritable(200));
    countDict.put("orange", new IntWritable(210));
    countDict.put("pear", new IntWritable(220));
    countDict.put("grape", new IntWritable(230));
    countDict.put("guava", new IntWritable(240));
    countDict.put("banana", new IntWritable(250));
    countDict.put("strawberry", new IntWritable(260));
  }
  @Test
  public void testCountDictEntropy() throws HiveException {
    CountDictEntropy udf = new CountDictEntropy();
    ObjectInspector[] arguments = {mapValueOI};
    udf.initialize(arguments);
    GenericUDF.DeferredObject[] args = {new GenericUDF.DeferredJavaObject(countDict)};
    DoubleWritable output = (DoubleWritable) udf.evaluate(args);
    assertEquals(output.get(), 1.9421210411202432);
  }

  @Test
  public void testCountDictMostFrequent() throws HiveException {
    CountDictMostFrequent udf = new CountDictMostFrequent();
    ObjectInspector[] arguments = {mapValueOI};
    udf.initialize(arguments);
    GenericUDF.DeferredObject[] args = {new GenericUDF.DeferredJavaObject(countDict)};
    Text output = (Text) udf.evaluate(args);
    assertEquals(output, new Text("strawberry"));
  }

  @Test
  public void testCountDictMostFrequentValue() throws HiveException {
    CountDictMostFrequentValue udf = new CountDictMostFrequentValue();
    ObjectInspector[] arguments = {mapValueOI};
    udf.initialize(arguments);
    GenericUDF.DeferredObject[] args = {new GenericUDF.DeferredJavaObject(countDict)};
    IntWritable output = (IntWritable) udf.evaluate(args);
    assertEquals(output.get(), 260);
  }
}
