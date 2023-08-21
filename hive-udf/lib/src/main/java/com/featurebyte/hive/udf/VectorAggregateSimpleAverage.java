package com.featurebyte.hive.udf;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;

/**
 * This class differs from VectorAggregateAverage in that the input here is just a single column of
 * a list, and not two columns.
 */
@SuppressWarnings("deprecation")
@Description(
    name = "vector_aggregate_simple_average",
    value = "_FUNC_(x) - Aggregate vectors by finding the average value")
public class VectorAggregateSimpleAverage extends BaseVectorAggregate {

  public VectorAggregateSimpleAverage() {}

  protected static class AverageAggregationBuffer
      extends GenericUDAFEvaluator.AbstractAggregationBuffer {

    List<Double> sumList;
    long count;

    public AverageAggregationBuffer() {
      sumList = new ArrayList<>();
      count = 0;
    }
  }

  protected GenericUDAFEvaluator getEvaluator() {
    return new VectorAggregateSimpleAverageFloatEvaluator();
  }

  public static class BaseVectorAggregateSimpleAverageEvaluator
      extends VectorAggregateListEvaluator {

    @Override
    public AggregationBuffer getNewAggregationBuffer() {
      return new AverageAggregationBuffer();
    }

    protected void doMerge(List<Double> listA, List<Double> listB) {
      // No-op because we override all the methods
    }

    protected void doIterate(AverageAggregationBuffer myagg, List<Object> myList, long count) {
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
      if (container.size() != myList.size()) {
        throw new RuntimeException(
            "The two lists are of different sizes. ListA: "
                + container.size()
                + ", ListB: "
                + myList.size());
      }

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
      assert (parameters.length == 1);

      Object partial = parameters[0];
      // Don't do anything if partial is empty.
      if (partial == null) {
        return;
      }

      // Cast current aggregation buffer, and partial value.
      AverageAggregationBuffer myagg = (AverageAggregationBuffer) agg;
      List<Object> myList = (List<Object>) partial;

      doIterate(myagg, myList, 1);
    }

    @Override
    public Object terminatePartial(AggregationBuffer agg) {
      // This function should produce the list of double writables.
      AverageAggregationBuffer myagg = (AverageAggregationBuffer) agg;
      Double[] doubleList = new Double[myagg.sumList.size() + 1];
      // Fill the first element with the count
      doubleList[0] = (double) myagg.count;

      // Convert sumList to double writable
      for (int i = 0; i < myagg.sumList.size(); i++) {
        Double currentSum = myagg.sumList.get(i);
        doubleList[i + 1] = currentSum;
      }

      return Arrays.asList(doubleList);
    }

    @Override
    public void merge(AggregationBuffer agg, Object partial) {
      // Don't do anything if partial is empty.
      if (partial == null) {
        return;
      }

      // Cast current aggregation buffer, and partial value.
      AverageAggregationBuffer myagg = (AverageAggregationBuffer) agg;
      List<Double> myList = (List<Double>) partial;

      // Get count from partial value
      Double partialCount = myList.get(0);
      long longPartialCount = partialCount.longValue();

      List<Object> sumList = new ArrayList<>(myList.subList(1, myList.size()));

      doIterate(myagg, sumList, longPartialCount);
    }

    @Override
    public Object terminate(AggregationBuffer agg) {
      AverageAggregationBuffer myagg = (AverageAggregationBuffer) agg;
      // Convert sumList to double writable
      Double[] doubleAverageList = new Double[myagg.sumList.size()];
      for (int i = 0; i < myagg.sumList.size(); i++) {
        Double currentSum = myagg.sumList.get(i);
        double currentAverage = currentSum / myagg.count;
        doubleAverageList[i] = currentAverage;
      }

      // Return list of averages
      return Arrays.asList(doubleAverageList);
    }
  }

  public static class VectorAggregateSimpleAverageFloatEvaluator
      extends BaseVectorAggregateSimpleAverageEvaluator {

    public VectorAggregateSimpleAverageFloatEvaluator() {}

    protected void doIterate(AverageAggregationBuffer myagg, List<Object> myList, long count) {
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
      if (container.size() != myList.size()) {
        throw new RuntimeException(
            "The two lists are of different sizes. ListA: "
                + container.size()
                + ", ListB: "
                + myList.size());
      }

      // Increase count
      myagg.count = myagg.count + count;
      // Add sum's on from the partial to the buffer.
      for (int i = 0; i < container.size(); i++) {
        Double containerCurrentValue = container.get(i);
        Double inputCurrentValue = Double.valueOf(myList.get(i).toString());
        myagg.sumList.set(i, containerCurrentValue + inputCurrentValue);
      }
    }
  }
}
