package com.featurebyte.hive.udf;

import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils.PrimitiveGrouping.NUMERIC_GROUP;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFUnixTimeStamp;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters.Converter;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;

@Description(
    name = "F_TIMESTAMP_TO_INDEX",
    value =
        "_FUNC_(eventTimestamp, timeModuloFrequencySeconds, blindSpotSeconds,"
            + " frequencyMinute) - returns the index of event timestamp given feature job"
            + " settings",
    extended =
        "Example:\n"
            + "  > SELECT F_TIMESTAMP_TO_INDEX('2020-10-05 10:00:00', 0, 120, 60);\n"
            + "  444970\n")
public class TimestampToIndex extends GenericUDF {
  private IntWritable output = new IntWritable();
  private transient Converter[] converters = new Converter[4];
  private transient PrimitiveCategory[] inputTypes = new PrimitiveCategory[4];
  private transient GenericUDFUnixTimeStamp unix_timestamp = new GenericUDFUnixTimeStamp();

  @Override
  public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
    checkArgsSize(arguments, 4, 4);

    ObjectInspector unix_timestamp_args[] = {arguments[0]};
    unix_timestamp.initialize(unix_timestamp_args);
    for (int i = 1; i < arguments.length; i++) {
      checkArgPrimitive(arguments, i);
      checkArgGroups(arguments, i, inputTypes, NUMERIC_GROUP);
      obtainIntConverter(arguments, i, inputTypes, converters);
    }

    return PrimitiveObjectInspectorFactory.writableIntObjectInspector;
  }

  @Override
  public Object evaluate(DeferredObject[] arguments) throws HiveException {
    if (arguments[0].get() == null) {
      return null;
    }
    DeferredObject unix_timestamp_args[] = {arguments[0]};

    long period_in_seconds = ((LongWritable) unix_timestamp.evaluate(unix_timestamp_args)).get();
    int time_modulo_frequency_seconds = getIntValue(arguments, 1, converters).intValue();
    int blind_spot_seconds = getIntValue(arguments, 2, converters).intValue();
    int frequency_minute = getIntValue(arguments, 3, converters).intValue();
    int offset = (time_modulo_frequency_seconds - blind_spot_seconds);
    output.set((int) Math.floor((period_in_seconds - offset) / (frequency_minute * 60)));
    return output;
  }

  @Override
  public String getDisplayString(String[] children) {
    return "F_TIMESTAMP_TO_INDEX";
  }
}
