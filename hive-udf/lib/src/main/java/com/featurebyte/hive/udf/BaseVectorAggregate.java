package com.featurebyte.hive.udf;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.udf.generic.AbstractGenericUDAFResolver;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFParameterInfo;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StandardListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;

import java.util.ArrayList;
import java.util.List;

import static org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category.LIST;

@Description(name = "vector_aggregate_sum", value = "_FUNC_(x) - Aggregate vectors by summing them")
@SuppressWarnings("deprecation")
public abstract class BaseVectorAggregate extends AbstractGenericUDAFResolver {

  public BaseVectorAggregate() {}

  /**
   * Return the evaluator that will be used.
   *
   * @return GenericUDAFEvaluator
   */
  protected abstract GenericUDAFEvaluator getEvaluator();

  public GenericUDAFEvaluator getEvaluator(GenericUDAFParameterInfo info)
    throws SemanticException {
    ObjectInspector firstParameter = getObjectInspector(info);
    StandardListObjectInspector listOI = (StandardListObjectInspector) firstParameter;
    String listElementTypeName = listOI.getListElementObjectInspector().getTypeName();
    PrimitiveObjectInspectorUtils.PrimitiveTypeEntry typeEntry =
      PrimitiveObjectInspectorUtils.getTypeEntryFromTypeName(listElementTypeName);
    switch (typeEntry.primitiveCategory) {
      case INT:
      case LONG:
      case FLOAT:
      case DOUBLE:
      case DECIMAL:
        return getEvaluator();
      default:
        throw new UDFArgumentTypeException(0, "Only ints, longs, floats, doubles or decimals are accepted " +
          "for parameter 1");
    }
  }

  private static ObjectInspector getObjectInspector(GenericUDAFParameterInfo info) throws SemanticException {
    // Taken from parent implementation
    if (info.isAllColumns()) {
      throw new SemanticException(
        "The specified syntax for UDAF invocation is invalid.");
    }

    ObjectInspector[] parameters = info.getParameterObjectInspectors();

    if (parameters.length != 1) {
      throw new UDFArgumentTypeException(
        parameters.length - 1, "Exactly one argument is expected.");
    }

    ObjectInspector firstParameter = parameters[0];

    if (firstParameter.getCategory() != LIST) {
      throw new UDFArgumentTypeException(0, "Parameter 1 must be a list");
    }
    return firstParameter;
  }

  static class ListAggregationBuffer extends GenericUDAFEvaluator.AbstractAggregationBuffer {

    List<Object> container;

    public ListAggregationBuffer() {
      container = new ArrayList<>();
    }
  }

  public static abstract class VectorAggregateListEvaluator extends GenericUDAFEvaluator {
    private transient ObjectInspector inputValueOI;

    public VectorAggregateListEvaluator() {}

    @Override
    public ObjectInspector init(Mode m, ObjectInspector[] parameters) throws HiveException {
      super.init(m, parameters);
      assert (parameters.length == 1);
      ObjectInspector inputListOI = parameters[0];
      StandardListObjectInspector internalValueOI = (StandardListObjectInspector) inputListOI;
      inputValueOI = internalValueOI.getListElementObjectInspector();
      return ObjectInspectorFactory.getStandardListObjectInspector(inputListOI);
    }

    protected ObjectInspector getInputValueOI() {
      return inputValueOI;
    }

    @Override
    public void reset(AggregationBuffer agg) {
      ((ListAggregationBuffer) agg).container.clear();
    }

    @Override
    public AggregationBuffer getNewAggregationBuffer() {
      return new ListAggregationBuffer();
    }

    @Override
    public void iterate(AggregationBuffer agg, Object[] parameters) {
      assert (parameters.length == 1);
      merge(agg, parameters[0]);
    }

    @Override
    public Object terminatePartial(AggregationBuffer agg) {
      ListAggregationBuffer myagg = (ListAggregationBuffer) agg;
      return new ArrayList<>(myagg.container);
    }

    protected abstract void doMerge(List<Object> listA, List<Object> listB);

    @Override
    public void merge(AggregationBuffer agg, Object partial) {
      // Don't do anything if partial is empty.
      if (partial == null) {
        return;
      }

      // Cast current aggregation buffer, and partial value.
      ListAggregationBuffer myagg = (ListAggregationBuffer) agg;
      List<Object> myList = (List<Object>) partial;
      List<Object> container = myagg.container;

      // If there's no current value in the buffer, just set the partial value into the buffer.
      if (container == null || container.isEmpty()) {
        myagg.container = myList;
        return;
      }

      // If not, compare the two lists, and update to the max value.
      assert (container.size() == myList.size());
      doMerge(container, myList);
    }

    @Override
    public Object terminate(AggregationBuffer agg) {
      return this.terminatePartial(agg);
    }
  }
}
