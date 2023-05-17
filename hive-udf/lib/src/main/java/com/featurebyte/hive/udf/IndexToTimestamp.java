package com.featurebyte.hive.udf;

import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils.PrimitiveGrouping.NUMERIC_GROUP;

import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFUnixTimeStamp;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters.Converter;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.Text;

@Description(
    name = "F_INDEX_TO_TIMESTAMP",
    value =
        "_FUNC_(tileIndex, timeModuloFrequencySeconds, blindSpotSeconds,"
            + " frequencyMinute) - returns the starting timestamp of a tile index given feature job"
            + " settings",
    extended =
        "Example:\n"
            + "  > SELECT F_INDEX_TO_TIMESTAMP(27585172, 15, 25, 1);\n"
            + "  \"2022-06-13T08:51:50.000Z\"\n")
public class IndexToTimestamp extends GenericUDF {
  private final Text output = new Text();
  private final transient Converter[] converters = new Converter[4];
  private final transient PrimitiveCategory[] inputTypes = new PrimitiveCategory[4];
  private final transient GenericUDFUnixTimeStamp unix_timestamp = new GenericUDFUnixTimeStamp();

  @Override
  public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
    checkArgsSize(arguments, 4, 4);

    checkArgGroups(arguments, 0, inputTypes, NUMERIC_GROUP);
    obtainLongConverter(arguments, 0, inputTypes, converters);

    for (int i = 1; i < arguments.length; i++) {
      checkArgPrimitive(arguments, i);
      checkArgGroups(arguments, i, inputTypes, NUMERIC_GROUP);
      obtainIntConverter(arguments, i, inputTypes, converters);
    }

    return PrimitiveObjectInspectorFactory.writableStringObjectInspector;
  }

  @Override
  public Object evaluate(DeferredObject[] arguments) throws HiveException {
    if (arguments[0].get() == null) {
      return null;
    }

    // There must be a better way to do this, but this works for now.
    long tile_index;
    try {
      tile_index = getLongValue(arguments, 0, converters);
    } catch (Exception e) {
      tile_index = getIntValue(arguments, 0, converters);
    }

    // The implementation follows Snowflake version of F_TIMESTAMP_TO_INDEX.
    int time_modulo_frequency_seconds = getIntValue(arguments, 1, converters);
    int blind_spot_seconds = getIntValue(arguments, 2, converters);
    int frequency_minute = getIntValue(arguments, 3, converters);
    int offset = (time_modulo_frequency_seconds - blind_spot_seconds);
    long period_in_seconds = (tile_index * (long) frequency_minute * (long) 60) + (long) offset;
    Timestamp ts = new Timestamp(period_in_seconds * 1000);

    // Convert to string format.
    String formattedTs = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSX").format(ts);
    output.set(formattedTs);
    return output;
  }

  @Override
  public String getDisplayString(String[] children) {
    return "F_INDEX_TO_TIMESTAMP";
  }
}
