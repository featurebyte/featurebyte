package com.featurebyte.hive.udf;

import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.serde2.objectinspector.*;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

public class ObjectAggregatorEvaluator extends GenericUDAFEvaluator
    implements Serializable {
  private static final long serialVersionUID = 21L;

  private transient ObjectInspector inputKeyOI;
  private transient ObjectInspector inputValueOI;
  private transient MapObjectInspector internalMergeOI;

  public ObjectAggregatorEvaluator() {
  }

  @Override
  public ObjectInspector init(Mode m, ObjectInspector[] parameters)
    throws HiveException {
    super.init(m, parameters);
    // init output object inspectors
    // The output of a partial aggregation is a list
    if (m == Mode.PARTIAL1) {
      inputKeyOI = ObjectInspectorUtils.getStandardObjectInspector(parameters[0]);
      inputValueOI = ObjectInspectorUtils.getStandardObjectInspector(parameters[1]);
      return ObjectInspectorFactory.getStandardMapObjectInspector(inputKeyOI, inputValueOI);
    } else {
      if (!(parameters[0] instanceof MapObjectInspector)) {
        //no map aggregation.
        inputKeyOI = ObjectInspectorUtils.getStandardObjectInspector(parameters[0]);
        inputValueOI = ObjectInspectorUtils.getStandardObjectInspector(parameters[1]);
        return ObjectInspectorFactory.getStandardMapObjectInspector(inputKeyOI, inputValueOI);
      } else {
        internalMergeOI = (MapObjectInspector) parameters[0];
        inputKeyOI = internalMergeOI.getMapKeyObjectInspector();
        inputValueOI = internalMergeOI.getMapValueObjectInspector();
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
  public void reset(AggregationBuffer agg) {
    ((ObjectAggregatorEvaluator.MapAggregationBuffer) agg).container.clear();
  }

  @Override
  public AggregationBuffer getNewAggregationBuffer() {
    return new ObjectAggregatorEvaluator.MapAggregationBuffer();
  }

  @Override
  public void iterate(AggregationBuffer agg, Object[] parameters) {
    assert (parameters.length == 2);
    String k = (String) parameters[0];
    Object v = parameters[1];

    if (k != null) {
      ObjectAggregatorEvaluator.MapAggregationBuffer myagg = (ObjectAggregatorEvaluator.MapAggregationBuffer) agg;
      putIntoCollection(k, v, myagg);
    }
  }

  @Override
  public Object terminatePartial(AggregationBuffer agg) {
    ObjectAggregatorEvaluator.MapAggregationBuffer myagg = (ObjectAggregatorEvaluator.MapAggregationBuffer) agg;
    return new HashMap<>(myagg.container);
  }

  @Override
  public void merge(AggregationBuffer agg, Object partial) {
    ObjectAggregatorEvaluator.MapAggregationBuffer myagg = (ObjectAggregatorEvaluator.MapAggregationBuffer) agg;
    Map<String, Object> partialResult = (Map<String, Object>) internalMergeOI.getMap(partial);
    if (partialResult != null) {
      for (Map.Entry<String, Object> set : partialResult.entrySet()) {
        putIntoCollection(set.getKey(), set.getValue(), myagg);
      }
    }
  }

  @Override
  public Object terminate(AggregationBuffer agg) {
    return this.terminatePartial(agg);
  }

  private void putIntoCollection(String k, Object v, ObjectAggregatorEvaluator.MapAggregationBuffer myagg) {
    String kCopy = (String) ObjectInspectorUtils.copyToStandardObject(k,  this.inputKeyOI);
    Object vCopy = ObjectInspectorUtils.copyToStandardObject(v,  this.inputValueOI);
    myagg.container.put(kCopy, vCopy);
  }

}
