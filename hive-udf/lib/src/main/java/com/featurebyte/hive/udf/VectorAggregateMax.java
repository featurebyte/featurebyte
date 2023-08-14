package com.featurebyte.hive.udf;

import java.util.List;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;

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
    public void doMerge(List<Object> listA, List<Object> listB) {
      ObjectInspector inputOI = getInputValueOI();
      for (int i = 0; i < listA.size(); i++) {
        Object containerCurrentValue = listA.get(i);
        Object inputCurrentValue = listB.get(i);
        int r =
            ObjectInspectorUtils.compare(
                containerCurrentValue, inputOI, inputCurrentValue, inputOI);
        if (r < 0) {
          listA.set(i, inputCurrentValue);
        }
      }
    }
  }
}
