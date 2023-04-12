package com.featurebyte.hive.udf;

import java.util.Map;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.objectinspector.*;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.IntWritable;

@Description(
    name = "F_COUNT_DICT_NUM_UNIQUE",
    value = "_FUNC_(counts) - compute number of unique keys in count dictionary")
public class CountDictNumUnique extends CountDictUDF {
  private final IntWritable output = new IntWritable();

  @Override
  public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
    checkArgsSize(arguments, 1, 1);
    if (isNullOI(arguments[0])) {
      return nullOI;
    }
    checkTypesAndInitialize(arguments);
    return PrimitiveObjectInspectorFactory.writableIntObjectInspector;
  }

  @Override
  public Object evaluate(DeferredObject[] arguments) throws HiveException {
    if (arguments[0].get() == null) {
      output.set(0);
      return output;
    }
    Map<String, Object> counts = (Map<String, Object>) inputMapOI.getMap(arguments[0].get());
    output.set(counts.size());
    return output;
  }

  @Override
  public String getDisplayString(String[] children) {
    return "F_COUNT_DICT_NUM_UNIQUE";
  }
}
