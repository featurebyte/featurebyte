package com.featurebyte.hive.udf;

import org.apache.hadoop.hive.serde2.objectinspector.*;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

import java.util.Map;


@Description(name = "F_COUNT_COSINE_SIMILARITY",
    value = "_FUNC_(counts) "
        + "- compute cosine similarity between two count dictionaries"
)
public class CountDictCosineSimilarity extends CountDictUDF {
  final private DoubleWritable output = new DoubleWritable();

  private transient MapObjectInspector otherInputMapOI;
  final private transient PrimitiveCategory[] otherInputTypes = new PrimitiveCategory[2];
  final private transient ObjectInspectorConverters.Converter[] otherConverters = new ObjectInspectorConverters.Converter[2];

  @Override
  public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
    checkArgsSize(arguments, 2, 2);
    if (isNullOI(arguments[0]) || isNullOI(arguments[1])) {
      return nullOI;
    }

    checkTypesAndInitialize(arguments);

    checkIsMap(arguments, 1);
    otherInputMapOI = checkTypesAndConstructMapOI(arguments[1], otherInputTypes, otherConverters);

    return PrimitiveObjectInspectorFactory.writableDoubleObjectInspector;
  }

  @Override
  public Object evaluate(DeferredObject[] arguments) throws HiveException {
    if (arguments[0].get() == null || arguments[1].get() == null) {
      return null;
    }
    Map<String, Object> counts1 = (Map<String, Object>) inputMapOI.getMap(arguments[0].get());
    Map<String, Object> counts2 = (Map<String, Object>) otherInputMapOI.getMap(arguments[1].get());
    if (counts1.size() == 0 ||  counts2.size() == 0) {
      output.set(0.0);
      return output;
    }
    Map<String, Object> counts;
    Map<String, Object> countsOther;
    if (counts1.size() < counts2.size()) {
      counts = counts1;
      countsOther = counts2;
    }
    else {
      counts = counts2;
      countsOther = counts1;
    }

    double dotProduct = 0.0;
    double norm = 0.0;
    double normOther = 0.0;

    for (String k : counts.keySet()) {
      double value = ((DoubleWritable) converters[1].convert(counts.get(k))).get();
      if (countsOther.containsKey(k)) {
        double valueOther = ((DoubleWritable) otherConverters[1].convert(countsOther.get(k))).get();
        dotProduct = dotProduct + value * valueOther;
      }
      norm = norm + value * value;
    }

    for (String k : countsOther.keySet()) {
      double value = ((DoubleWritable) otherConverters[1].convert(countsOther.get(k))).get();
      normOther = normOther + value * value;
    }

    output.set(dotProduct / (Math.sqrt(norm) * Math.sqrt(normOther)));
    return output;
  }

  @Override
  public String getDisplayString(String[] children) {
      return "F_COUNT_DICT_COSINE_SIMILARITY";
  }
}
