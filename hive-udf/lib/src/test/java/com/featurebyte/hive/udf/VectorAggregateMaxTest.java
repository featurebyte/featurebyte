package com.featurebyte.hive.udf;

import java.util.Arrays;
import java.util.List;

public class VectorAggregateMaxTest extends BaseVectorAggregateListTest {

  protected BaseVectorAggregate getAggregator() {
    return new VectorAggregateMax();
  }

  protected List<Double> getResults() {
    return Arrays.asList(100d, 101d, 102d);
  }

}
