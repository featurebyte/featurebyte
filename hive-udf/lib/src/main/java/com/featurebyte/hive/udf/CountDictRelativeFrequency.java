package com.featurebyte.hive.udf;

import org.apache.hadoop.hive.serde2.objectinspector.*;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;

import java.util.Map;

@Description(name = "F_GET_RELATIVE_FREQUENCY",
  value = "_FUNC_(counts, key) "
    + "- compute relative frequency of a key in a dictionary"
)
public class CountDictRelativeFrequency extends CountDictUDF {

  final private DoubleWritable output = new DoubleWritable();

  final private transient PrimitiveObjectInspector.PrimitiveCategory[] stringInputTypes = new PrimitiveObjectInspector.PrimitiveCategory[3];
  final private transient ObjectInspectorConverters.Converter[] stringConverters = new ObjectInspectorConverters.Converter[1];

  @Override
  public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {

    // Input arguments: dictionary, key of interest
    checkArgsSize(arguments, 2, 2);
    if (isNullOI(arguments[0])) {
      return nullOI;
    }

    // Map
    checkTypesAndInitialize(arguments);

    // Key
    if (isNullOI(arguments[1])) {
      return nullOI;
    }
    ObjectInspector[] args = {arguments[1]};
    checkArgPrimitive(args, 0);
    obtainStringConverter(args, 0, stringInputTypes, stringConverters);

    return PrimitiveObjectInspectorFactory.writableDoubleObjectInspector;
  }

  @Override
  public Object evaluate(DeferredObject[] arguments) throws HiveException {
    if (arguments[0].get() == null || arguments[1].get() == null) {
      return null;
    }
    Map<String, Object> counts = (Map<String, Object>) inputMapOI.getMap(arguments[0].get());
    String key = stringConverters[0].convert(arguments[1].get()).toString();
    if (!counts.containsKey(key)) {
      return null;
    }
    double keyValue = ((DoubleWritable) converters[1].convert(counts.get(key))).get();
    double total = 0.0;
    for (Object value : counts.values()) {
      if (value != null) {
        double doubleValue = ((DoubleWritable) converters[1].convert(value)).get();
        total += doubleValue;
      }
    }
    output.set(keyValue / total);
    return output;
  }

  @Override
  public String getDisplayString(String[] children) {
    return "F_GET_RELATIVE_FREQUENCY";
  }
}
