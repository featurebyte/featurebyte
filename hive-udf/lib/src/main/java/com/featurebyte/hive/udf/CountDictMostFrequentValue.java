package com.featurebyte.hive.udf;

import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.serde2.objectinspector.*;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;

import java.util.Map;

import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils.PrimitiveGrouping.NUMERIC_GROUP;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils.PrimitiveGrouping.STRING_GROUP;

@Description(name = "F_COUNT_DICT_MOST_FREQUENT_VALUE",
  value = "_FUNC_(counts) "
    + "- compute most frequent value from count dictionary"
)
public class CountDictMostFrequentValue extends GenericUDF {
  private transient MapObjectInspector inputMapOI;
  final private transient PrimitiveCategory[] inputTypes = new PrimitiveCategory[2];
  final private transient ObjectInspectorConverters.Converter[] converters = new ObjectInspectorConverters.Converter[2];

  @Override
  public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
    checkArgsSize(arguments, 1, 1);
    if (!(arguments[0] instanceof MapObjectInspector)) {
      throw new UDFArgumentTypeException(0, "Parameter 1 must be a Map");
    }
    inputMapOI = (MapObjectInspector) arguments[0];
    ObjectInspector[] map_args = {
      inputMapOI.getMapKeyObjectInspector(),
      inputMapOI.getMapValueObjectInspector()
    };

    try {
      checkArgPrimitive(map_args,0);
      checkArgGroups(map_args, 0, inputTypes, STRING_GROUP);
    } catch (UDFArgumentException e) {
      throw new UDFArgumentTypeException(0, "Map key must be a string");
    }

    try {
      checkArgPrimitive(map_args,1);
      checkArgGroups(map_args, 1, inputTypes, NUMERIC_GROUP);
      obtainDoubleConverter(map_args,1, inputTypes, converters);
    } catch (UDFArgumentException e) {
      throw new UDFArgumentTypeException(0, "Map value must be numeric");
    }

    return inputMapOI.getMapValueObjectInspector();
  }

  @Override
  public Object evaluate(DeferredObject[] arguments) throws HiveException {
    if (arguments[0] == null) {
      return null;
    }
    Map<String, Object> counts = (Map<String, Object>) inputMapOI.getMap(arguments[0].get());
    double most_frequent_count = 0.0;
    Object most_frquent_count_value = null;
    for (Map.Entry<String, Object> entry : counts.entrySet()) {
      double doubleValue = ((DoubleWritable) converters[1].convert(entry.getValue())).get();
      if (doubleValue > most_frequent_count) {
        most_frequent_count = doubleValue;
        most_frquent_count_value = entry.getValue();
      }
    }
    return most_frquent_count_value;
  }

  @Override
  public String getDisplayString(String[] children) {
    return "F_COUNT_DICT_MOST_FREQUENT_VALUE";
  }
}
