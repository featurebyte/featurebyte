package com.featurebyte.hive.udf;

import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.DoubleWritable;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class CountDictEntropyTest {
  CountDictEntropy udf = new CountDictEntropy();
  @Test
  public void testCountDictEntropy() throws HiveException {

    ObjectInspector mapValueOI = ObjectInspectorFactory.getStandardMapObjectInspector(
      PrimitiveObjectInspectorFactory.writableStringObjectInspector,
      PrimitiveObjectInspectorFactory.writableDoubleObjectInspector
    );
    ObjectInspector[] arguments = {mapValueOI};
    udf.initialize(arguments);

    Map<String, DoubleWritable> countDict = new HashMap<>();
    countDict.put("apple", new DoubleWritable(200d));
    countDict.put("orange", new DoubleWritable(210d));
    countDict.put("pear", new DoubleWritable(220d));
    countDict.put("grape", new DoubleWritable(230d));
    countDict.put("guava", new DoubleWritable(240d));
    countDict.put("banana", new DoubleWritable(250d));
    countDict.put("strawberry", new DoubleWritable(260d));
    GenericUDF.DeferredObject[] args = {new GenericUDF.DeferredJavaObject(countDict)};
    DoubleWritable output = (DoubleWritable) udf.evaluate(args);
    assertEquals(output.get(), 1.9421210411202432);
  }

}
