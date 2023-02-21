package com.featurebyte.hive.udf;

import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.serde2.objectinspector.*;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

import java.util.Map;

import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils.PrimitiveGrouping.NUMERIC_GROUP;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils.PrimitiveGrouping.STRING_GROUP;

@Description(name = "F_COUNT_COSINE_SIMILARITY",
    value = "_FUNC_(counts) "
        + "- compute cosine similarity between two count dictionaries"
)
public class CountDictCosineSimilarity extends GenericUDF {
  final private DoubleWritable output = new DoubleWritable();
  private transient MapObjectInspector inputMapOI;
  final private transient PrimitiveCategory[] inputTypes = new PrimitiveCategory[2];
  final private transient ObjectInspectorConverters.Converter[] converters = new ObjectInspectorConverters.Converter[2];

  @Override
  public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
    checkArgsSize(arguments, 2, 2);
    if (!(arguments[0] instanceof MapObjectInspector)) {
      throw new UDFArgumentTypeException(0, "Parameter 1 must be a Map");
    }
    if (!(arguments[1] instanceof MapObjectInspector)) {
      throw new UDFArgumentTypeException(0, "Parameter 2 must be a Map");
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

    return PrimitiveObjectInspectorFactory.writableDoubleObjectInspector;
  }

  @Override
  public Object evaluate(DeferredObject[] arguments) throws HiveException {
    if (arguments[0] == null || arguments[1] == null) {
      return null;
    }
    Map<String, Object> counts1 = (Map<String, Object>) inputMapOI.getMap(arguments[0].get());
    Map<String, Object> counts2 = (Map<String, Object>) inputMapOI.getMap(arguments[1].get());
    if (counts1.size() == 0 ||  counts2.size() == 0) {
      return 0;
    }
    Map<String, Object> counts;
    Map<String, Object> counts_other;
    if (counts1.size() < counts2.size()) {
      counts = counts1;
      counts_other = counts2;
    }
    else {
      counts = counts2;
      counts_other = counts1;
    }

    double dot_product = 0.0;
    double norm = 0.0;
    double norm_other = 0.0;

    for (String k : counts.keySet()) {
      double value = ((DoubleWritable) converters[1].convert(counts.get(k))).get();
      if (counts_other.containsKey(k)) {
        double value_other = ((DoubleWritable) converters[1].convert(counts_other.get(k))).get();
        dot_product = dot_product + value * value_other;
      }
      norm = norm + value * value;
    }

    for (String k : counts_other.keySet()) {
      double value = ((DoubleWritable) converters[1].convert(counts_other.get(k))).get();
      norm_other = norm_other + value * value;
    }

    output.set(dot_product / (Math.sqrt(norm) * Math.sqrt(norm_other)));
    return output;
  }

  @Override
  public String getDisplayString(String[] children) {
      return "F_COUNT_DICT_COSINE_SIMILARITY";
  }
}
