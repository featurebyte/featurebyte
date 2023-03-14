package com.featurebyte.hive.udf;

import org.apache.hadoop.hive.serde2.objectinspector.*;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

import java.util.Map;

@Description(name = "F_COUNT_DICT_ENTROPY",
    value = "_FUNC_(counts) "
        + "- compute entropy value from count dictionary"
)
public class CountDictEntropy extends CountDictUDF {
  final private DoubleWritable output = new DoubleWritable();

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
    int index = 0;
    double total = 0.0;
    double[] values = new double[counts.size()];
    for (Object value : counts.values()) {
      if (value != null) {
        double doubleValue = convertMapValueAsDouble(value);
        total += doubleValue;
        values[index++] = doubleValue;
      }
    }

    double entropy = 0.0;
    int count_length = values.length;
    for (index = 0; index < count_length; index++) {
      double p = values[index] / total;
      entropy += p * Math.log(p);
    }
    entropy *= -1.0;
    output.set(entropy);
    return output;
  }

  @Override
  public String getDisplayString(String[] children) {
      return "F_COUNT_DICT_ENTROPY";
  }
}
