package com.featurebyte.hive.udf;

import java.util.List;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;

@Description(name = "vector_aggregate_sum", value = "_FUNC_(x) - Aggregate vectors by summing them")
public class VectorAggregateSum extends BaseVectorAggregate {

  public VectorAggregateSum() {}

  public GenericUDAFEvaluator getEvaluator() {
    return new VectorAggregateSumEvaluator();
  }

  public static class VectorAggregateSumEvaluator extends VectorAggregateListEvaluator {

    @Override
    public void doMerge(List<Double> listA, List<Double> listB) {
      for (int i = 0; i < listA.size(); i++) {
        listA.set(i, listA.get(i) + listB.get(i));
      }
    }
  }
}
