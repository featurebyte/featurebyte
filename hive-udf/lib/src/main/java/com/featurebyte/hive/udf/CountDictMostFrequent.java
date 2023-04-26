package com.featurebyte.hive.udf;

import java.util.Map;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.objectinspector.*;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.Text;

@Description(
    name = "F_COUNT_DICT_MOST_FREQUENT",
    value = "_FUNC_(counts) - compute most frequent value from count dictionary")
public class CountDictMostFrequent extends CountDictUDF {
  private final Text output = new Text();

  @Override
  public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
    checkArgsSize(arguments, 1, 1);
    if (isNullOI(arguments[0])) {
      return nullOI;
    }
    checkTypesAndInitialize(arguments);
    return PrimitiveObjectInspectorFactory.writableStringObjectInspector;
  }

  @Override
  public Object evaluate(DeferredObject[] arguments) throws HiveException {
    if (arguments[0].get() == null) {
      return null;
    }
    Map<String, Object> counts = (Map<String, Object>) inputMapOI.getMap(arguments[0].get());
    String mostFrequentKey = null;
    double mostFrequentValue = 0.0;
    for (Map.Entry<String, Object> entry : counts.entrySet()) {
      double doubleValue = convertMapValueAsDouble(entry.getValue());
      if (Double.isNaN(doubleValue)) continue;
      String key = entry.getKey();
      if (doubleValue > mostFrequentValue) {
        mostFrequentValue = doubleValue;
        mostFrequentKey = key;
      } else if (doubleValue == mostFrequentValue
          && mostFrequentKey != null
          && key.compareTo(mostFrequentKey) < 0) {
        mostFrequentKey = key;
      }
    }

    if (mostFrequentKey == null) return null;

    output.set(mostFrequentKey);
    return output;
  }

  @Override
  public String getDisplayString(String[] children) {
    return "F_COUNT_DICT_MOST_FREQUENT";
  }
}
