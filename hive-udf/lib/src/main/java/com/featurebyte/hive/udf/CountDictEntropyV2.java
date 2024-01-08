package com.featurebyte.hive.udf;

import static com.featurebyte.hive.udf.UDFUtils.isNullOI;
import static com.featurebyte.hive.udf.UDFUtils.nullOI;

import java.util.Map;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.hive.serde2.objectinspector.*;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

@Description(
    name = "F_COUNT_DICT_ENTROPY",
    value = "_FUNC_(counts) - compute entropy value from count dictionary")
public class CountDictEntropyV2 extends CountDictUDFV1 {
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
    int index = 0;
    double total = 0.0;
    double[] values = new double[counts.size()];
    for (Object value : counts.values()) {
      if (value != null) {
        double doubleValue = convertMapValueAsDouble(value);
        if (Double.isNaN((doubleValue)) || doubleValue == 0) continue;
        total += doubleValue;
        values[index++] = doubleValue;
      }
    }

    double entropy = 0.0;
    int count_length = index;
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
