package com.featurebyte.hive.udf;

import java.util.Arrays;
import java.util.List;

public class VectorAggregateMaxTest extends BaseVectorAggregateListTest {

  protected BaseVectorAggregateV1 getAggregator() {
    return new VectorAggregateMaxV1();
  }

  protected List<Double> getResults() {
    return Arrays.asList(100d, 101d, 102d);
  }
}
