package com.featurebyte.hive.udf;

import org.apache.hadoop.hive.serde2.objectinspector.*;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;

import java.util.Map;

@Description(name = "F_COUNT_DICT_MOST_FREQUENT_VALUE",
  value = "_FUNC_(counts) "
    + "- compute most frequent value from count dictionary"
)
public class CountDictMostFrequentValue extends CountDictUDF {

  @Override
  public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
    checkArgsSize(arguments, 1, 1);
    if (isNullOI(arguments[0])) {
      return nullOI;
    }
    checkTypesAndInitialize(arguments);
    return inputMapOI.getMapValueObjectInspector();
  }

  @Override
  public Object evaluate(DeferredObject[] arguments) throws HiveException {
    if (arguments[0].get() == null) {
      return null;
    }
    Map<String, Object> counts = (Map<String, Object>) inputMapOI.getMap(arguments[0].get());
    double most_frequent_count = 0.0;
    Object most_frequent_count_value = null;
    for (Map.Entry<String, Object> entry : counts.entrySet()) {
      double doubleValue = convertMapValueAsDouble(entry.getValue());
      if (doubleValue > most_frequent_count) {
        most_frequent_count = doubleValue;
        most_frequent_count_value = entry.getValue();
      }
    }
    return most_frequent_count_value;
  }

  @Override
  public String getDisplayString(String[] children) {
    return "F_COUNT_DICT_MOST_FREQUENT_VALUE";
  }
}
