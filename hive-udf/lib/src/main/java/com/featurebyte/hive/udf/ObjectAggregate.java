package com.featurebyte.hive.udf;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.udf.generic.AbstractGenericUDAFResolver;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.serde2.objectinspector.*;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;

import java.util.HashMap;
import java.util.Map;

import static org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category.PRIMITIVE;

@Description(name = "object_agg", value = "_FUNC_(x) - Aggregate objects")
public class ObjectAggregate extends AbstractGenericUDAFResolver {

  public ObjectAggregate() {
  }

  @Override
  public GenericUDAFEvaluator getEvaluator(TypeInfo[] parameters)
    throws SemanticException {
    if (parameters.length != 2) {
      throw new UDFArgumentTypeException(parameters.length - 1,
        "Exactly two arguments are expected.");
    }

    if (parameters[0].getCategory() != PRIMITIVE) {
      throw new UDFArgumentTypeException(0, "Parameter 1 must be a primitive");
    }

    if (parameters[1].getCategory() != PRIMITIVE) {
      throw new UDFArgumentTypeException(0, "Parameter 2 must be a primitive");
    }

    switch (((PrimitiveTypeInfo) parameters[0]).getPrimitiveCategory()) {
      case STRING:
      case VARCHAR:
        return new ObjectAggregatorEvaluator();
      default:
        throw new UDFArgumentTypeException(0, "Only string is accepted for parameter 1");
    }
  }

  static class MapAggregationBuffer extends GenericUDAFEvaluator.AbstractAggregationBuffer {

    final private Map<Object, Object> container;

    public MapAggregationBuffer() {
      container = new HashMap<>();
    }
  }

  public static class ObjectAggregatorEvaluator extends GenericUDAFEvaluator {
    private transient ObjectInspector inputKeyOI;
    private transient ObjectInspector inputValueOI;
    private transient MapObjectInspector internalMergeOI;
    public ObjectAggregatorEvaluator() {
    }

    @Override
    public ObjectInspector init(Mode m, ObjectInspector[] parameters)
      throws HiveException {
      super.init(m, parameters);
      if (m == Mode.PARTIAL1 || m == Mode.COMPLETE) {
        // original inputs
        assert (parameters.length == 2);
        inputKeyOI = parameters[0];
        inputValueOI = parameters[1];
      } else {
        // merge inputs
        internalMergeOI = (MapObjectInspector) parameters[0];
        inputKeyOI = internalMergeOI.getMapKeyObjectInspector();
        inputValueOI = internalMergeOI.getMapValueObjectInspector();
      }

      // partial and final aggregation returns map
      return ObjectInspectorFactory.getStandardMapObjectInspector(inputKeyOI, inputValueOI);
    }

    @Override
    public void reset(AggregationBuffer agg) {
      ((MapAggregationBuffer) agg).container.clear();
    }

    @Override
    public AggregationBuffer getNewAggregationBuffer() {
      return new MapAggregationBuffer();
    }

    @Override
    public void iterate(AggregationBuffer agg, Object[] parameters) {
      assert (parameters.length == 2);
      Object k = parameters[0];
      Object v = parameters[1];

      if (k != null) {
        MapAggregationBuffer myagg = (MapAggregationBuffer) agg;
        putIntoCollection(k, v, myagg);
      }
    }

    @Override
    public Object terminatePartial(AggregationBuffer agg) {
      MapAggregationBuffer myagg = (MapAggregationBuffer) agg;
      return new HashMap<>(myagg.container);
    }

    @Override
    public void merge(AggregationBuffer agg, Object partial) {
      MapAggregationBuffer myagg = (MapAggregationBuffer) agg;
      Map<Object, Object> partialResult = (Map<Object, Object>) internalMergeOI.getMap(partial);
      if (partialResult != null) {
        for (Map.Entry<Object, Object> set : partialResult.entrySet()) {
          putIntoCollection(set.getKey(), set.getValue(), myagg);
        }
      }
    }

    @Override
    public Object terminate(AggregationBuffer agg) {
      return this.terminatePartial(agg);
    }

    private void putIntoCollection(Object k, Object v, MapAggregationBuffer myagg) {
      myagg.container.put(k, v);
    }
  }

}
