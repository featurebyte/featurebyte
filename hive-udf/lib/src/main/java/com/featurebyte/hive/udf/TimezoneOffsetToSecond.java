package com.featurebyte.hive.udf;

import java.time.ZoneOffset;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.IntWritable;

@Description(
    name = "F_TIMEZONE_OFFSET_TO_SECOND",
    value = "_FUNC_(timezoneOffsetString) - returns the number of seconds of timezone offset",
    extended = "Example:\n" + "  > SELECT F_TIMEZONE_OFFSET_TO_SECOND('+08:00');\n" + "  480\n")
public class TimezoneOffsetToSecond extends GenericUDF {
  private final IntWritable output = new IntWritable();

  protected final transient PrimitiveObjectInspector.PrimitiveCategory[] stringInputTypes =
      new PrimitiveObjectInspector.PrimitiveCategory[1];

  protected final transient ObjectInspectorConverters.Converter[] stringConverters =
      new ObjectInspectorConverters.Converter[1];

  @Override
  public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
    checkArgsSize(arguments, 1, 1);

    ObjectInspector[] args = {arguments[0]};
    checkArgPrimitive(args, 0);
    obtainStringConverter(args, 0, stringInputTypes, stringConverters);

    return PrimitiveObjectInspectorFactory.writableIntObjectInspector;
  }

  @Override
  public Object evaluate(DeferredObject[] arguments) throws HiveException {
    if (arguments[0].get() == null) {
      return null;
    }
    String timezoneOffsetString = stringConverters[0].convert(arguments[0].get()).toString();
    ZoneOffset zoneOffset = ZoneOffset.of(timezoneOffsetString);
    output.set(zoneOffset.getTotalSeconds());
    return output;
  }

  @Override
  public String getDisplayString(String[] children) {
    return "F_TIMEZONE_OFFSET_TO_SECOND";
  }
}
