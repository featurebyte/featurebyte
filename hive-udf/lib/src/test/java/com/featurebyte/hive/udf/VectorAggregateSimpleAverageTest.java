package com.featurebyte.hive.udf;

import java.util.Arrays;
import java.util.List;

public class VectorAggregateSimpleAverageTest extends BaseVectorAggregateListTest {

  protected BaseVectorAggregate getAggregator() {
    return new VectorAggregateSimpleAverage();
  }

  protected List<Double> getResults() {
    return Arrays.asList(20d, 21d, 22d);
  }
}
