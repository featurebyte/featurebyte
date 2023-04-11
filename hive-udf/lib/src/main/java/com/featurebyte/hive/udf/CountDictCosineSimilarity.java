package com.featurebyte.hive.udf;

import java.util.Map;
import java.util.Set;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.objectinspector.*;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.DoubleWritable;

@Description(
    name = "F_COUNT_COSINE_SIMILARITY",
    value = "_FUNC_(counts) " + "- compute cosine similarity between two count dictionaries")
public class CountDictCosineSimilarity extends CountDictUDF {
  private final DoubleWritable output = new DoubleWritable();

  private transient MapObjectInspector otherInputMapOI;
  private final transient PrimitiveCategory[] otherInputTypes = new PrimitiveCategory[2];
  private final transient ObjectInspectorConverters.Converter[] otherConverters =
      new ObjectInspectorConverters.Converter[2];

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
    if (counts1.size() == 0 || counts2.size() == 0) {
      output.set(0.0);
      return output;
    }
    Map<String, Object> counts;
    Map<String, Object> countsOther;
    ObjectInspectorConverters.Converter converter;
    ObjectInspectorConverters.Converter converterOther;
    if (counts1.size() < counts2.size()) {
      counts = counts1;
      countsOther = counts2;
      converter = converters[1];
      converterOther = otherConverters[1];
    } else {
      counts = counts2;
      countsOther = counts1;
      converter = otherConverters[1];
      converterOther = converters[1];
    }

    double dotProduct = 0.0;
    double norm = 0.0;
    double normOther = 0.0;

    for (Map.Entry<String, Object> set : counts.entrySet()) {
      DoubleWritable count = (DoubleWritable) converter.convert(set.getValue());
      if (count == null) continue;
      double value = count.get();
      if (Double.isNaN(value)) continue;
      Object objectOther = countsOther.getOrDefault(set.getKey(), null);
      if (objectOther != null) {
        DoubleWritable countOther = (DoubleWritable) converterOther.convert(objectOther);
        if (countOther == null) continue;
        double valueOther = countOther.get();
        if (Double.isNaN(valueOther)) continue;
        dotProduct = dotProduct + value * valueOther;
        normOther += valueOther * valueOther;
      }
      norm += value * value;
    }

    Set<String> keySet = countsOther.keySet();
    keySet.removeAll(counts.keySet());
    for (String k : keySet) {
      DoubleWritable count = (DoubleWritable) converterOther.convert(countsOther.get(k));
      if (count == null) continue;
      double value = count.get();
      normOther += value * value;
    }

    output.set(dotProduct / (Math.sqrt(norm) * Math.sqrt(normOther)));
    return output;
  }

  @Override
  public String getDisplayString(String[] children) {
    return "F_COUNT_DICT_COSINE_SIMILARITY";
  }
}
