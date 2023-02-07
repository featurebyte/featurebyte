package com.featurebyte.hive.udf;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.udf.generic.AbstractGenericUDAFResolver;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.serde2.objectinspector.*;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.WritableIntObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.io.IntWritable;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

import static org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category.PRIMITIVE;

@Description(name = "mode", value = "_FUNC_(x) - Get most common value")
public class Mode extends AbstractGenericUDAFResolver {

  public Mode() {
  }

  @Override
  public GenericUDAFEvaluator getEvaluator(TypeInfo[] parameters)
    throws SemanticException {
    if (parameters.length != 1) {
      throw new UDFArgumentTypeException(parameters.length - 1,
        "Exactly one arguments is expected.");
    }

    if (parameters[0].getCategory() != PRIMITIVE) {
      throw new UDFArgumentTypeException(0, "Parameter 1 must be a primitive");
    }
    return new ModeEvaluator();
  }

  static class ModeAggregationBuffer extends GenericUDAFEvaluator.AbstractAggregationBuffer {

    final private Map<Object, Object> container;

    public ModeAggregationBuffer() {
      container = new HashMap<>();
    }
  }

  public static class ModeEvaluator extends GenericUDAFEvaluator {
    private transient PrimitiveObjectInspector inputValueOI;
    private transient WritableIntObjectInspector countValueOI;
    private transient MapObjectInspector internalMergeOI;
    public transient Object result;

    public ModeEvaluator() {
    }

    @Override
    public ObjectInspector init(GenericUDAFEvaluator.Mode m, ObjectInspector[] parameters)
      throws HiveException {
      super.init(m, parameters);
      if (m == Mode.PARTIAL1 || m == Mode.COMPLETE) {
        // original inputs
        assert (parameters.length == 1);
        inputValueOI = (PrimitiveObjectInspector) parameters[0];
        countValueOI = PrimitiveObjectInspectorFactory.writableIntObjectInspector;
      } else {
        // merge inputs
        internalMergeOI = (MapObjectInspector) parameters[0];
        inputValueOI = (PrimitiveObjectInspector) internalMergeOI.getMapKeyObjectInspector();
        countValueOI = (WritableIntObjectInspector) internalMergeOI.getMapValueObjectInspector();
      }

      if (m == Mode.PARTIAL1 || m == Mode.PARTIAL2) {
        // partial aggregation returns map
        return ObjectInspectorFactory.getStandardMapObjectInspector(inputValueOI, countValueOI);
      }

      // final aggregation returns object
      result = null;
      return inputValueOI;
    }

    @Override
    public void reset(AggregationBuffer agg) {
      ((ModeAggregationBuffer) agg).container.clear();
    }

    @Override
    public AggregationBuffer getNewAggregationBuffer() {
      return new ModeAggregationBuffer();
    }

    @Override
    public void iterate(AggregationBuffer agg, Object[] parameters) {
      assert (parameters.length == 1);
      Object v = parameters[0];
      if (v != null) {
        ModeAggregationBuffer myagg = (ModeAggregationBuffer) agg;
        putIntoCollection(v, null, myagg);
      }
    }

    @Override
    public Object terminatePartial(AggregationBuffer agg) {
      ModeAggregationBuffer myagg = (ModeAggregationBuffer) agg;
      return new HashMap<>(myagg.container);
    }

    @Override
    public void merge(AggregationBuffer agg, Object partial) {
      ModeAggregationBuffer myagg = (ModeAggregationBuffer) agg;
      Map<Object, Object> partialResult = (Map<Object, Object>) internalMergeOI.getMap(partial);
      if (partialResult != null) {
        for (Map.Entry<Object, Object> set : partialResult.entrySet()) {
          putIntoCollection(set.getKey(), set.getValue(), myagg);
        }
      }
    }

    @Override
    public Object terminate(AggregationBuffer agg) {
      ModeAggregationBuffer myagg = (ModeAggregationBuffer) agg;
      int max_count = 0;
      for (Map.Entry<Object, Object> set : myagg.container.entrySet()) {
        if (set != null) {
          int count = countValueOI.get(set.getValue());
          if (count > max_count) {
            result = set.getKey();
            max_count = count;
          }
        }
      }
      return result;
    }

    private void putIntoCollection(Object v, Object count, ModeAggregationBuffer myagg) {
      Object vCopy = ObjectInspectorUtils.copyToStandardObject(v, inputValueOI);
      int newCount = count != null ? countValueOI.get(count) : 1;
      if (myagg.container.containsKey(vCopy)) {
        newCount += countValueOI.get(myagg.container.get(vCopy));
      }
      myagg.container.put(vCopy, countValueOI.create(newCount));
    }
  }

}
