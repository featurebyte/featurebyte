package com.featurebyte.hive.udf;

import static com.featurebyte.hive.udf.UDFUtils.isNullOI;
import static com.featurebyte.hive.udf.UDFUtils.nullOI;

import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.Text;

@Description(
    name = "F_COUNT_DICT_NORMALIZE",
    value = "_FUNC_(counts) - normalize count dictionary values to sum to 1")
public class CountDictNormalizeV1 extends CountDictUDFV1 {

  @Override
  public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
    checkArgsSize(arguments, 1, 1);
    if (isNullOI(arguments[0])) {
      return nullOI;
    }
    checkTypesAndInitialize(arguments);
    return ObjectInspectorFactory.getStandardMapObjectInspector(
        PrimitiveObjectInspectorFactory.writableStringObjectInspector,
        PrimitiveObjectInspectorFactory.writableDoubleObjectInspector);
  }

  @Override
  public Object evaluate(DeferredObject[] arguments) throws HiveException {
    if (arguments[0].get() == null) {
      return null;
    }
    Map<String, Object> counts = (Map<String, Object>) inputMapOI.getMap(arguments[0].get());

    // Calculate the total sum
    double total = 0.0;
    for (Object value : counts.values()) {
      if (value != null) {
        double doubleValue = convertMapValueAsDouble(value);
        if (!Double.isNaN(doubleValue)) {
          total += doubleValue;
        }
      }
    }

    // Return empty map if total is 0
    if (total == 0) {
      return new HashMap<Text, DoubleWritable>();
    }

    // Normalize the values
    HashMap<Text, DoubleWritable> result = new HashMap<>();
    for (Map.Entry<String, Object> entry : counts.entrySet()) {
      if (entry.getValue() != null) {
        double doubleValue = convertMapValueAsDouble(entry.getValue());
        if (!Double.isNaN(doubleValue)) {
          result.put(new Text(entry.getKey()), new DoubleWritable(doubleValue / total));
        }
      }
    }
    return result;
  }

  @Override
  public String getDisplayString(String[] children) {
    return "F_COUNT_DICT_NORMALIZE";
  }
}
