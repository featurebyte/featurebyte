package com.featurebyte.hive.udf;

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.objectinspector.*;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.DoubleWritable;

@Description(
    name = "F_GET_RANK",
    value = "_FUNC_(counts, key, isDescending) " + "- compute rank of a key in a dictionary")
public class CountDictRank extends CountDictSingleStringArgumentUDF {

  private final DoubleWritable output = new DoubleWritable();

  @Override
  public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
    checkArgsSize(arguments, 3, 3);
    // Map
    if (isNullOI(arguments[0])) {
      return nullOI;
    }
    // Key
    if (isNullOI(arguments[1])) {
      return nullOI;
    }
    checkTypesAndInitialize(arguments);
    return PrimitiveObjectInspectorFactory.writableDoubleObjectInspector;
  }

  @Override
  public Object evaluate(DeferredObject[] arguments) throws HiveException {
    if (arguments[0].get() == null || arguments[1].get() == null) {
      return null;
    }
    Map<String, Object> counts = (Map<String, Object>) inputMapOI.getMap(arguments[0].get());
    String key = getStringArgument(arguments);
    if (!counts.containsKey(key)) {
      return null;
    }
    boolean isDescending =
        PrimitiveObjectInspectorFactory.writableBooleanObjectInspector.get(arguments[2].get());
    double direction = isDescending ? -1.0 : 1.0;
    List<Map.Entry<String, Object>> sortedCounts =
        counts.entrySet().stream()
            .sorted(Comparator.comparing(e -> direction * convertMapValueAsDouble(e.getValue())))
            .collect(Collectors.toList());

    double currentRank = 0.0;
    double previousValue = 0.0;
    for (int i = 0; i < sortedCounts.size(); i++) {
      double currentValue = convertMapValueAsDouble(sortedCounts.get(i).getValue());
      // If values are the same, we will return the same rank. E.g. for {"a": 20, "b": 20,
      // "c": 10},
      // "a" and "b" will have the same rank 1, and "c" will have rank 3.
      if (i == 0 || currentValue != previousValue) {
        currentRank = i + 1.0;
        previousValue = currentValue;
      }
      if (sortedCounts.get(i).getKey().equals(key)) {
        output.set(currentRank);
        return output;
      }
    }
    return null;
  }

  @Override
  public String getDisplayString(String[] children) {
    return "F_GET_RANK";
  }
}
