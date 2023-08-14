package com.featurebyte.hive.udf;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;

import java.util.List;

@Description(name = "vector_aggregate_sum", value = "_FUNC_(x) - Aggregate vectors by summing them")
public class VectorAggregateSum extends BaseVectorAggregate {

  public VectorAggregateSum() {}

  public GenericUDAFEvaluator getEvaluator() {
    return new VectorAggregateSumEvaluator();
  }

  public static class VectorAggregateSumEvaluator extends VectorAggregateListEvaluator {

    @Override
    public void doMerge(List<Object> listA, List<Object> listB) {
      for (int i = 0; i < listA.size(); i++) {
        Object containerCurrentValue = listA.get(i);
        Object inputCurrentValue = listB.get(i);
        double currentValue = Double.parseDouble(containerCurrentValue.toString());
        double inputValue = Double.parseDouble(inputCurrentValue.toString());
        listA.set(i, currentValue + inputValue);
      }
    }
  }
}
