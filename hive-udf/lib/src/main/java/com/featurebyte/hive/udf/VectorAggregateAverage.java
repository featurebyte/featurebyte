package com.featurebyte.hive.udf;

import static org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category.LIST;
import static org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category.PRIMITIVE;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
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
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;

@Description(
    name = "vector_aggregate_average",
    value = "_FUNC_(x) - Aggregate vectors by taking the average")
@SuppressWarnings("deprecation")
public class VectorAggregateAverage extends AbstractGenericUDAFResolver {

  public VectorAggregateAverage() {}

  public GenericUDAFEvaluator getEvaluator(GenericUDAFParameterInfo info) throws SemanticException {
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
        return new VectorAggregateAverageEvaluator();
      default:
        throw new UDFArgumentTypeException(
            0, "Only ints, longs, floats, doubles or decimals are accepted " + "for parameter 1");
    }
  }

  private static void assertParameter(
      ObjectInspector parameter, int argumentNumber, ObjectInspector.Category expectedType)
      throws UDFArgumentTypeException {
    if (parameter.getCategory() != expectedType) {
      throw new UDFArgumentTypeException(argumentNumber, "Parameter must be a list");
    }
  }

  private static ObjectInspector getObjectInspector(GenericUDAFParameterInfo info)
      throws SemanticException {
    // Taken from parent implementation
    if (info.isAllColumns()) {
      throw new SemanticException("The specified syntax for UDAF invocation is invalid.");
    }

    ObjectInspector[] parameters = info.getParameterObjectInspectors();

    if (parameters.length != 2) {
      throw new UDFArgumentTypeException(
          parameters.length - 2, "Exactly two arguments are expected.");
    }

    ObjectInspector firstParameter = parameters[0];
    assertParameter(firstParameter, 0, LIST);
    assertParameter(parameters[1], 1, PRIMITIVE);
    return firstParameter;
  }

  static class VectorAvgAggregationBuffer extends GenericUDAFEvaluator.AbstractAggregationBuffer {

    List<Double> sumList;
    long count;

    public VectorAvgAggregationBuffer() {
      sumList = new ArrayList<>();
      count = 0;
    }
  }

  public static class VectorAggregateAverageEvaluator extends GenericUDAFEvaluator {

    public VectorAggregateAverageEvaluator() {}

    @Override
    public ObjectInspector init(Mode m, ObjectInspector[] parameters) throws HiveException {
      super.init(m, parameters);

      // Assert that there are 2 parameters if it's the original data
      assert m != Mode.PARTIAL1 && m != Mode.COMPLETE || parameters.length == 2;

      // We always returns a list of doubles regardless of whether it's the partial aggregation, or
      // final aggregation
      // The main difference is that the output of a partial aggregation is a list of doubles
      // representing the count and sum of elements being averaged. The first element in the list is
      // the count of elements, and the rest of the elements are the sums for
      // the respective items.
      return ObjectInspectorFactory.getStandardListObjectInspector(
          PrimitiveObjectInspectorFactory.javaDoubleObjectInspector);
    }

    @Override
    public void reset(AggregationBuffer agg) {
      ((VectorAvgAggregationBuffer) agg).sumList.clear();
    }

    @Override
    public AggregationBuffer getNewAggregationBuffer() {
      return new VectorAvgAggregationBuffer();
    }

    private void doIterate(VectorAvgAggregationBuffer myagg, List<Object> myList, long count) {
      List<Double> container = myagg.sumList;

      // If there's no current value in the buffer, just set the partial value into the buffer.
      if (container == null || container.isEmpty()) {
        List<Double> convertedList = new ArrayList<>();
        for (Object currentValue : myList) {
          convertedList.add(Double.valueOf(currentValue.toString()));
        }
        myagg.count = count;
        myagg.sumList = convertedList;
        return;
      }

      // If not, compare the two lists, and update to the max value.
      assert (container.size() == myList.size());

      // Increase count
      myagg.count = myagg.count + count;
      // Add sum's on from the partial to the buffer.
      for (int i = 0; i < container.size(); i++) {
        Double containerCurrentValue = container.get(i);
        Double inputCurrentValue = Double.valueOf(myList.get(i).toString());
        myagg.sumList.set(i, containerCurrentValue + inputCurrentValue);
      }
    }

    @Override
    public void iterate(AggregationBuffer agg, Object[] parameters) {
      assert (parameters.length == 2);

      Object partial = parameters[0];
      // Don't do anything if partial is empty.
      if (partial == null) {
        return;
      }

      // Cast current aggregation buffer, and partial value.
      VectorAvgAggregationBuffer myagg = (VectorAvgAggregationBuffer) agg;
      List<Object> myList = (List<Object>) partial;
      Integer count = (Integer) parameters[1];

      doIterate(myagg, myList, count.longValue());
    }

    @Override
    public Object terminatePartial(AggregationBuffer agg) {
      // This function should produce the list of double writables.
      VectorAvgAggregationBuffer myagg = (VectorAvgAggregationBuffer) agg;
      Double[] doubleList = new Double[myagg.sumList.size() + 1];
      // Fill the first element with the count
      doubleList[0] = (double) myagg.count;

      // Convert sumList to double writable
      for (int i = 0; i < myagg.sumList.size(); i++) {
        Double currentSum = myagg.sumList.get(i);
        doubleList[i + 1] = currentSum;
      }

      return doubleList;
    }

    @Override
    public void merge(AggregationBuffer agg, Object partial) {
      // Don't do anything if partial is empty.
      if (partial == null) {
        return;
      }

      // Cast current aggregation buffer, and partial value.
      VectorAvgAggregationBuffer myagg = (VectorAvgAggregationBuffer) agg;
      Double[] myList = (Double[]) partial;

      // Get count from partial value
      Double partialCount = myList[0];
      long longPartialCount = partialCount.longValue();

      List<Object> sumList = new ArrayList<>(Arrays.asList(myList).subList(1, myList.length));

      doIterate(myagg, sumList, longPartialCount);
    }

    @Override
    public Object terminate(AggregationBuffer agg) {
      VectorAvgAggregationBuffer myagg = (VectorAvgAggregationBuffer) agg;
      // Convert sumList to double writable
      Double[] doubleAverageList = new Double[myagg.sumList.size()];
      for (int i = 0; i < myagg.sumList.size(); i++) {
        Double currentSum = myagg.sumList.get(i);
        double currentAverage = currentSum / myagg.count;
        doubleAverageList[i] = currentAverage;
      }

      // Return list of averages
      return doubleAverageList;
    }
  }
}
