package com.featurebyte.hive.udf;

import java.util.Arrays;
import java.util.List;

public class VectorAggregateSumTest extends BaseVectorAggregateListTest {
  protected BaseVectorAggregateV1 getAggregator() {
    return new VectorAggregateSumV1();
  }

  protected List<Double> getResults() {
    return Arrays.asList(140d, 147d, 154d);
  }
}
