package com.featurebyte.hive.udf;

import java.util.Arrays;
import java.util.List;

public class VectorAggregateSumTest extends BaseVectorAggregateListTest {
  protected BaseVectorAggregate getAggregator() {
    return new VectorAggregateSum();
  }

  protected List<Double> getResults() {
    return Arrays.asList(140d, 147d, 154d);
  }
}
