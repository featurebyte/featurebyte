package com.featurebyte.hive.udf;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.apache.hadoop.hive.common.type.Timestamp;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF.DeferredObject;
import org.apache.hadoop.hive.serde2.io.TimestampWritableV2;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.IntWritable;
import org.junit.jupiter.api.Test;

public class TimestampToIndexTest {
  TimestampToIndex udf = new TimestampToIndex();

  @Test
  public void testConvertTimestampToIndex() throws HiveException {
    ObjectInspector tsValueOI = PrimitiveObjectInspectorFactory.writableTimestampObjectInspector;
    ObjectInspector intValueOI = PrimitiveObjectInspectorFactory.writableIntObjectInspector;
    ObjectInspector[] arguments = {tsValueOI, intValueOI, intValueOI, intValueOI};
    udf.initialize(arguments);

    Timestamp ts = Timestamp.valueOf("2020-10-05 10:00:00");
    DeferredObject eventTimestamp = new GenericUDF.DeferredJavaObject(new TimestampWritableV2(ts));
    DeferredObject timeModuloFrequencySeconds =
        new GenericUDF.DeferredJavaObject(new IntWritable(0));
    DeferredObject blindSpotSeconds = new GenericUDF.DeferredJavaObject(new IntWritable(120));
    DeferredObject frequencyMinute = new GenericUDF.DeferredJavaObject(new IntWritable(60));
    DeferredObject[] args = {
      eventTimestamp, timeModuloFrequencySeconds, blindSpotSeconds, frequencyMinute
    };
    IntWritable output = (IntWritable) udf.evaluate(args);
    assertEquals(output.get(), 444970);
  }
}
