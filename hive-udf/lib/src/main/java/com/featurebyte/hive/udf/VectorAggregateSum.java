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
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.StandardListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;

import java.util.ArrayList;
import java.util.List;

import static org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category.LIST;

@Description(name = "vector_aggregate_sum", value = "_FUNC_(x) - Aggregate vectors by summing them")
@SuppressWarnings("deprecation")
public class VectorAggregateSum extends AbstractGenericUDAFResolver {

  public VectorAggregateSum() {}

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
        return new VectorAggregateSumEvaluator();
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

  static class SumAggregationBuffer extends GenericUDAFEvaluator.AbstractAggregationBuffer {

    List<Object> container;

    public SumAggregationBuffer() {
      container = new ArrayList<>();
    }
  }

  public static class VectorAggregateSumEvaluator extends GenericUDAFEvaluator {
    private transient ObjectInspector inputValueOI;

    public VectorAggregateSumEvaluator() {}

    @Override
    public ObjectInspector init(Mode m, ObjectInspector[] parameters) throws HiveException {
      super.init(m, parameters);
      assert (parameters.length == 1);
      ObjectInspector inputListOI = parameters[0];
      StandardListObjectInspector internalValueOI = (StandardListObjectInspector) inputListOI;
      inputValueOI = internalValueOI.getListElementObjectInspector();
      return ObjectInspectorFactory.getStandardListObjectInspector(inputListOI);
    }

    @Override
    public void reset(AggregationBuffer agg) {
      ((SumAggregationBuffer) agg).container.clear();
    }

    @Override
    public AggregationBuffer getNewAggregationBuffer() {
      return new SumAggregationBuffer();
    }

    @Override
    public void iterate(AggregationBuffer agg, Object[] parameters) {
      assert (parameters.length == 1);
      merge(agg, parameters[0]);
    }

    @Override
    public Object terminatePartial(AggregationBuffer agg) {
      SumAggregationBuffer myagg = (SumAggregationBuffer) agg;
      return new ArrayList<>(myagg.container);
    }

    @Override
    public void merge(AggregationBuffer agg, Object partial) {
      // Don't do anything if partial is empty.
      if (partial == null) {
        return;
      }

      // Cast current aggregation buffer, and partial value.
      SumAggregationBuffer myagg = (SumAggregationBuffer) agg;
      List<Object> myList = (List<Object>) partial;
      List<Object> container = myagg.container;

      // If there's no current value in the buffer, just set the partial value into the buffer.
      if (container == null || container.isEmpty()) {
        myagg.container = myList;
        return;
      }

      // If not, compare the two lists, and update to the sum value.
      assert (container.size() == myList.size());

      for (int i = 0; i < container.size(); i++) {
        Object containerCurrentValue = container.get(i);
        Object inputCurrentValue = myList.get(i);
        double currentValue = Double.parseDouble(containerCurrentValue.toString());
        double inputValue = Double.parseDouble(inputCurrentValue.toString());
        container.set(i, currentValue + inputValue);
      }
    }

    @Override
    public Object terminate(AggregationBuffer agg) {
      return this.terminatePartial(agg);
    }
  }
}
