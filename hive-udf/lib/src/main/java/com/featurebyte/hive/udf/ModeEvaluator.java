package com.featurebyte.hive.udf;

import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.serde2.objectinspector.MapObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.WritableIntObjectInspector;
import org.apache.hadoop.io.IntWritable;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

public class ModeEvaluator extends GenericUDAFEvaluator
  implements Serializable {

  private static final long serialVersionUID = 22L;
  private transient ObjectInspector inputValueOI;
  private transient ObjectInspector countValueOI = PrimitiveObjectInspectorFactory.writableIntObjectInspector;
  private transient MapObjectInspector internalMergeOI;

  public ModeEvaluator() {
  }

  @Override
  public ObjectInspector init(GenericUDAFEvaluator.Mode m, ObjectInspector[] parameters)
    throws HiveException {
    super.init(m, parameters);
    // init output object inspectors
    // The output of a partial aggregation is a list
    if (m == GenericUDAFEvaluator.Mode.PARTIAL1) {
      inputValueOI = ObjectInspectorUtils.getStandardObjectInspector(parameters[0]);
      return ObjectInspectorFactory.getStandardMapObjectInspector(inputValueOI, countValueOI);
    } else {
      if (!(parameters[0] instanceof MapObjectInspector)) {
        //no map aggregation.
        inputValueOI = ObjectInspectorUtils.getStandardObjectInspector(parameters[0]);
        return ObjectInspectorFactory.getStandardMapObjectInspector(inputValueOI, countValueOI);
      } else {
        internalMergeOI = (MapObjectInspector) parameters[0];
        inputValueOI = internalMergeOI.getMapKeyObjectInspector();
        countValueOI = internalMergeOI.getMapValueObjectInspector();
        return ObjectInspectorUtils.getStandardObjectInspector(internalMergeOI);
      }
    }
  }

  static class MapAggregationBuffer extends AbstractAggregationBuffer {

    final private Map<Object, Object> container;

    public MapAggregationBuffer() {
      container = new HashMap<>();
    }
  }

  @Override
  public void reset(GenericUDAFEvaluator.AggregationBuffer agg) {
    ((ModeEvaluator.MapAggregationBuffer) agg).container.clear();
  }

  @Override
  public GenericUDAFEvaluator.AggregationBuffer getNewAggregationBuffer() {
    return new ModeEvaluator.MapAggregationBuffer();
  }

  @Override
  public void iterate(GenericUDAFEvaluator.AggregationBuffer agg, Object[] parameters) {
    assert (parameters.length == 1);
    Object v = parameters[0];
    if (v != null) {
      ModeEvaluator.MapAggregationBuffer myagg = (ModeEvaluator.MapAggregationBuffer) agg;
      putIntoCollection(v, null, myagg);
    }
  }

  @Override
  public Object terminatePartial(AggregationBuffer agg) {
    ModeEvaluator.MapAggregationBuffer myagg = (ModeEvaluator.MapAggregationBuffer) agg;
    return new HashMap<>(myagg.container);
  }

  @Override
  public void merge(GenericUDAFEvaluator.AggregationBuffer agg, Object partial) {
    ModeEvaluator.MapAggregationBuffer myagg = (ModeEvaluator.MapAggregationBuffer) agg;
    Map<Object, Object> partialResult = (Map<Object, Object>) internalMergeOI.getMap(partial);
    if (partialResult != null) {
      for (Map.Entry<Object, Object> set : partialResult.entrySet()) {
        putIntoCollection(set.getKey(), set.getValue(), myagg);
      }
    }
  }

  @Override
  public Object terminate(GenericUDAFEvaluator.AggregationBuffer agg) {
    ModeEvaluator.MapAggregationBuffer myagg = (ModeEvaluator.MapAggregationBuffer) agg;
    Object mode = null;
    int max_count = 0;
    for (Map.Entry<Object, Object> set : myagg.container.entrySet()) {
      if (set != null) {
        int count = ((IntWritable)set.getValue()).get();
        if (count > max_count) {
          mode = set.getKey();
          max_count = count;
        }
      }
    }
    return mode;
  }

  private void putIntoCollection(Object v, Object count, ModeEvaluator.MapAggregationBuffer myagg) {
    Object vCopy = ObjectInspectorUtils.copyToStandardObject(v, this.inputValueOI);
    if (myagg.container.containsKey(vCopy)) {
      IntWritable currentCount = (IntWritable) myagg.container.get(vCopy);
      int countInt = count != null ? ((IntWritable) count).get() : 1;
      currentCount.set(currentCount.get() + countInt);
    } else {
      myagg.container.put(vCopy, count != null ? count : new IntWritable(1));
    }
  }

}
