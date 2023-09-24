package com.featurebyte.hive.udf;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.Text;

@Description(
    name = "F_COUNT_DICT_LEQST_FREQUENT",
    value =
        "_FUNC_(counts) - compute least frequent key / key with lowest value from count dictionary")
public class CountDictLeastFrequent extends CountDictMostFrequentKeyValue {
  private final Text output = new Text();

  public ObjectInspector getOutputOI() {
    return PrimitiveObjectInspectorFactory.writableStringObjectInspector;
  }

  public boolean isReversed() {
    return true;
  }

  @Override
  public Object evaluate(DeferredObject[] arguments) throws HiveException {

    if (evaluateAsKeyValue(arguments) == null) return null;

    output.set(outputKey);
    return output;
  }

  @Override
  public String getDisplayString(String[] children) {
    return "F_COUNT_DICT_MOST_FREQUENT";
  }
}
