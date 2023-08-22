package com.featurebyte.hive.udf;

import java.util.List;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;

@Description(
    name = "vector_aggregate_max",
    value = "_FUNC_(x) - Aggregate vectors by selecting the maximum value")
public class VectorAggregateMax extends BaseVectorAggregate {

  public VectorAggregateMax() {}

  protected GenericUDAFEvaluator getEvaluator() {
    return new VectorAggregateMaxEvaluator();
  }

  public static class VectorAggregateMaxEvaluator extends VectorAggregateListEvaluator {

    public VectorAggregateMaxEvaluator() {}

    @Override
    public void doMerge(List<Double> listA, List<Double> listB) {
      for (int i = 0; i < listA.size(); i++) {
        double valueA = listA.get(i);
        double valueB = listB.get(i);
        if (valueA < valueB) {
          listA.set(i, valueB);
        }
      }
    }
  }
}
