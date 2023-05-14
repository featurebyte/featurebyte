package com.featurebyte.hive.udf;

import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF.DeferredObject;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.IntWritable;
import org.junit.jupiter.api.Test;

public class IndexToTimestampTest {
  IndexToTimestamp udf = new IndexToTimestamp();

  @Test
  public void testConvertIndexToTimestamp() throws HiveException {
    ObjectInspector tileIndexValueOI = PrimitiveObjectInspectorFactory.writableIntObjectInspector;
    ObjectInspector intValueOI = PrimitiveObjectInspectorFactory.writableIntObjectInspector;
    ObjectInspector[] arguments = {tileIndexValueOI, intValueOI, intValueOI, intValueOI};
    udf.initialize(arguments);

    DeferredObject tileIndex = new GenericUDF.DeferredJavaObject(new IntWritable(27585172));
    DeferredObject timeModuloFrequencySeconds =
        new GenericUDF.DeferredJavaObject(new IntWritable(15));
    DeferredObject blindSpotSeconds = new GenericUDF.DeferredJavaObject(new IntWritable(25));
    DeferredObject frequencyMinute = new GenericUDF.DeferredJavaObject(new IntWritable(1));
    DeferredObject[] args = {
      tileIndex, timeModuloFrequencySeconds, blindSpotSeconds, frequencyMinute
    };
    String outputTs = udf.evaluate(args).toString();
    // Only compare the date part of the timestamp due to timezone differences. When used in
    // featurebyte, the session timezone is always set to UTC, so this should return the correct
    // result ending with a "Z". More detailed tests are done in featurebyte repo.
    assertTrue(outputTs.startsWith("2022-06-13T"));
  }
}
