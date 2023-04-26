package com.featurebyte.hive.udf;

import java.util.Map;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.hive.serde2.objectinspector.*;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

@Description(
    name = "F_COUNT_DICT_MOST_FREQUENT_VALUE",
    value = "_FUNC_(counts) - compute most frequent value from count dictionary")
public class CountDictMostFrequentValue extends CountDictUDF {

  private final DoubleWritable output = new DoubleWritable();

  @Override
  public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
    checkArgsSize(arguments, 1, 1);
    if (isNullOI(arguments[0])) {
      return nullOI;
    }
    checkTypesAndInitialize(arguments);
    return PrimitiveObjectInspectorFactory.writableDoubleObjectInspector;
  }

  @Override
  public Object evaluate(DeferredObject[] arguments) throws HiveException {
    if (arguments[0].get() == null) {
      return null;
    }
    Map<String, Object> counts = (Map<String, Object>) inputMapOI.getMap(arguments[0].get());

    String mostFrequentKey = null;
    double mostFrequentCount = 0.0;
    for (Map.Entry<String, Object> entry : counts.entrySet()) {
      double doubleValue = convertMapValueAsDouble(entry.getValue());
      if (Double.isNaN(doubleValue)) continue;
      if (doubleValue > mostFrequentCount) {
        mostFrequentKey = entry.getKey();
        mostFrequentCount = doubleValue;
      }
    }
    if (mostFrequentKey == null) return null;

    output.set(mostFrequentCount);
    return output;
  }

  @Override
  public String getDisplayString(String[] children) {
    return "F_COUNT_DICT_MOST_FREQUENT_VALUE";
  }
}
