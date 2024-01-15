package com.featurebyte.hive.udf;

import static com.featurebyte.hive.udf.UDFUtils.isNullOI;
import static com.featurebyte.hive.udf.UDFUtils.nullOI;

import java.util.Map;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.io.Text;

public abstract class CountDictMostFrequentKeyValueV1 extends CountDictUDFV1 {

  protected final Text outputKey = new Text();
  protected final DoubleWritable outputValue = new DoubleWritable();

  @Override
  public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
    checkArgsSize(arguments, 1, 1);
    if (isNullOI(arguments[0])) {
      return nullOI;
    }
    checkTypesAndInitialize(arguments);
    return getOutputOI();
  }

  public abstract ObjectInspector getOutputOI();

  // Whether to invert the count values before comparison. If true, the result will be the least
  // frequent key.
  public abstract boolean isReversed();

  public Object evaluateAsKeyValue(DeferredObject[] arguments) throws HiveException {
    if (arguments[0].get() == null) {
      return null;
    }
    Map<String, Object> counts = (Map<String, Object>) inputMapOI.getMap(arguments[0].get());
    String mostFrequentKey = null;
    double mostFrequentValue = Double.NEGATIVE_INFINITY;
    for (Map.Entry<String, Object> entry : counts.entrySet()) {
      double doubleValue = convertMapValueAsDouble(entry.getValue());
      if (Double.isNaN(doubleValue)) continue;
      if (isReversed()) {
        doubleValue = -1.0 * doubleValue;
      }
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

    outputKey.set(mostFrequentKey);
    outputValue.set(mostFrequentValue);

    return mostFrequentKey;
  }
}
