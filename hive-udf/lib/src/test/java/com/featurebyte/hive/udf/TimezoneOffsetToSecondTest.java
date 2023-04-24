package com.featurebyte.hive.udf;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.IntWritable;
import org.junit.jupiter.api.Test;

public class TimezoneOffsetToSecondTest {
  TimezoneOffsetToSecond udf = new TimezoneOffsetToSecond();

  @Test
  public void testConvertTimestampToIndex_1() throws HiveException {
    ObjectInspector offsetValueOI = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
    ObjectInspector[] arguments = {offsetValueOI};
    udf.initialize(arguments);

    GenericUDF.DeferredObject timezoneOffsetValue = new GenericUDF.DeferredJavaObject("+01:00");
    GenericUDF.DeferredObject[] args = {timezoneOffsetValue};
    IntWritable output = (IntWritable) udf.evaluate(args);
    assertEquals(output.get(), 3600);
  }

  @Test
  public void testConvertTimestampToIndex_2() throws HiveException {
    ObjectInspector offsetValueOI = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
    ObjectInspector[] arguments = {offsetValueOI};
    udf.initialize(arguments);

    GenericUDF.DeferredObject timezoneOffsetValue = new GenericUDF.DeferredJavaObject("-05:30");
    GenericUDF.DeferredObject[] args = {timezoneOffsetValue};
    IntWritable output = (IntWritable) udf.evaluate(args);
    assertEquals(output.get(), -(5 * 3600 + 30 * 60));
  }
}
