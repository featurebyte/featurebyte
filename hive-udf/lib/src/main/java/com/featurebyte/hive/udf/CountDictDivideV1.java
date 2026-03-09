package com.featurebyte.hive.udf;

import static com.featurebyte.hive.udf.UDFUtils.isNullOI;
import static com.featurebyte.hive.udf.UDFUtils.nullOI;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils.PrimitiveGrouping.NUMERIC_GROUP;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils.PrimitiveGrouping.VOID_GROUP;

import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.Text;

@Description(
    name = "F_COUNT_DICT_DIVIDE",
    value = "_FUNC_(counts, divisor) - divide all values in a count dictionary by a numeric divisor")
public class CountDictDivideV1 extends CountDictUDFV1 {

  private final transient PrimitiveObjectInspector.PrimitiveCategory[] divisorInputTypes =
      new PrimitiveObjectInspector.PrimitiveCategory[1];
  private final transient ObjectInspectorConverters.Converter[] divisorConverters =
      new ObjectInspectorConverters.Converter[1];

  @Override
  public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
    checkArgsSize(arguments, 2, 2);
    if (isNullOI(arguments[0])) {
      return nullOI;
    }
    if (isNullOI(arguments[1])) {
      return nullOI;
    }
    checkTypesAndInitialize(arguments);
    return ObjectInspectorFactory.getStandardMapObjectInspector(
        PrimitiveObjectInspectorFactory.writableStringObjectInspector,
        PrimitiveObjectInspectorFactory.writableDoubleObjectInspector);
  }

  @Override
  protected void checkTypesAndInitialize(ObjectInspector[] arguments) throws UDFArgumentException {
    // Map
    super.checkTypesAndInitialize(arguments);

    // Divisor (numeric)
    ObjectInspector[] args = {arguments[1]};
    checkArgPrimitive(args, 0);
    checkArgGroups(args, 0, divisorInputTypes, NUMERIC_GROUP, VOID_GROUP);
    obtainDoubleConverter(args, 0, divisorInputTypes, divisorConverters);
  }

  @Override
  public Object evaluate(DeferredObject[] arguments) throws HiveException {
    if (arguments[0].get() == null || arguments[1].get() == null) {
      return null;
    }
    DoubleWritable divisorWritable =
        (DoubleWritable) divisorConverters[0].convert(arguments[1].get());
    if (divisorWritable == null) {
      return null;
    }
    double divisor = divisorWritable.get();
    if (divisor == 0 || Double.isNaN(divisor)) {
      return null;
    }

    Map<String, Object> counts = (Map<String, Object>) inputMapOI.getMap(arguments[0].get());

    HashMap<Text, DoubleWritable> result = new HashMap<>();
    for (Map.Entry<String, Object> entry : counts.entrySet()) {
      if (entry.getValue() != null) {
        double doubleValue = convertMapValueAsDouble(entry.getValue());
        if (!Double.isNaN(doubleValue)) {
          result.put(new Text(entry.getKey()), new DoubleWritable(doubleValue / divisor));
        }
      }
    }
    return result;
  }

  @Override
  public String getDisplayString(String[] children) {
    return "F_COUNT_DICT_DIVIDE";
  }
}
