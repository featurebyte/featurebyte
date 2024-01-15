package com.featurebyte.hive.udf;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.hive.serde2.objectinspector.*;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

@Description(
    name = "F_COUNT_DICT_MOST_FREQUENT_VALUE",
    value = "_FUNC_(counts) - compute most frequent value from count dictionary")
public class CountDictMostFrequentValueV1 extends CountDictMostFrequentKeyValueV1 {

  private final DoubleWritable output = new DoubleWritable();

  @Override
  public ObjectInspector getOutputOI() {
    return PrimitiveObjectInspectorFactory.writableDoubleObjectInspector;
  }

  @Override
  public boolean isReversed() {
    return false;
  }

  @Override
  public Object evaluate(DeferredObject[] arguments) throws HiveException {

    if (evaluateAsKeyValue(arguments) == null) return null;

    output.set(outputValue.get());
    return output;
  }

  @Override
  public String getDisplayString(String[] children) {
    return "F_COUNT_DICT_MOST_FREQUENT_VALUE";
  }
}
