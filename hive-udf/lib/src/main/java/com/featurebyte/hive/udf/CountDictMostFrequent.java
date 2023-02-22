package com.featurebyte.hive.udf;

import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.serde2.objectinspector.*;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

import java.util.Map;

import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils.PrimitiveGrouping.NUMERIC_GROUP;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils.PrimitiveGrouping.STRING_GROUP;

@Description(name = "F_COUNT_DICT_MOST_FREQUENT",
  value = "_FUNC_(counts) "
    + "- compute most frequent value from count dictionary"
)
public class CountDictMostFrequent extends CountDictUDF {
  final private Text output = new Text();
  private transient MapObjectInspector inputMapOI;
  final private transient PrimitiveCategory[] inputTypes = new PrimitiveCategory[2];
  final private transient ObjectInspectorConverters.Converter[] converters = new ObjectInspectorConverters.Converter[2];

  @Override
  public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
    checkArgsSize(arguments, 1, 1);
    if (isNullOI(arguments[0])) {
      return nullOI;
    }
    checkIsMap(arguments, 0);
    inputMapOI = checkTypesAndConstructMapOI(arguments[0], inputTypes, converters);
    return PrimitiveObjectInspectorFactory.writableStringObjectInspector;
  }

  @Override
  public Object evaluate(DeferredObject[] arguments) throws HiveException {
    if (arguments[0].get() == null) {
      return null;
    }
    Map<String, Object> counts = (Map<String, Object>) inputMapOI.getMap(arguments[0].get());
    String most_frequent_key = null;
    double most_frequent_count = 0.0;
    for (Map.Entry<String, Object> entry : counts.entrySet()) {
      double doubleValue = ((DoubleWritable) converters[1].convert(entry.getValue())).get();
      if (doubleValue > most_frequent_count) {
        most_frequent_count = doubleValue;
        most_frequent_key = entry.getKey();
      }
    }

    output.set(most_frequent_key);
    return output;
  }

  @Override
  public String getDisplayString(String[] children) {
    return "F_COUNT_DICT_MOST_FREQUENT";
  }
}
