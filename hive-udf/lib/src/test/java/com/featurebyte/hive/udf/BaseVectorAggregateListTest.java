package com.featurebyte.hive.udf;

import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.ql.udf.generic.SimpleGenericUDAFParameterInfo;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

@SuppressWarnings("deprecation")
public abstract class BaseVectorAggregateListTest {

  protected abstract BaseVectorAggregate getAggregator();

  /**
   * getResults should return the final aggregated result. Input is hardcoded into the test, so
   * refer to those values to calculate your final result.
   */
  protected abstract List<Double> getResults();

  @Test
  public void testErrorThrownIfColumnHasArrayOfDifferentLengths() throws HiveException {
    ObjectInspector[] doubleOI =
      new ObjectInspector[] {
        ObjectInspectorFactory.getStandardListObjectInspector(
          PrimitiveObjectInspectorFactory.javaDoubleObjectInspector),
      };
    BaseVectorAggregate udaf = getAggregator();
    SimpleGenericUDAFParameterInfo info =
      new SimpleGenericUDAFParameterInfo(doubleOI, false, false, false);
    GenericUDAFEvaluator eval1 = udaf.getEvaluator(info);
    GenericUDAFEvaluator.AggregationBuffer buffer1 = eval1.getNewAggregationBuffer();
    eval1.iterate(buffer1, new Object[] {Arrays.asList(1d, 2d, 3d)});
    try {
      eval1.iterate(buffer1, new Object[] {Arrays.asList(4d, 5d, 6d, 7d)});
      throw new RuntimeException("iterate should throw an assertion error as the size of the array passed in is " +
        "different");
    } catch (AssertionError ignored) {
    }
  }

  @Test
  public void testVectorAggregate() throws HiveException {
    ObjectInspector[] doubleOI =
        new ObjectInspector[] {
          ObjectInspectorFactory.getStandardListObjectInspector(
              PrimitiveObjectInspectorFactory.javaDoubleObjectInspector),
        };
    BaseVectorAggregate udaf = getAggregator();
    SimpleGenericUDAFParameterInfo info =
        new SimpleGenericUDAFParameterInfo(doubleOI, false, false, false);
    GenericUDAFEvaluator eval1 = udaf.getEvaluator(info);
    GenericUDAFEvaluator eval2 = udaf.getEvaluator(info);

    eval1.init(GenericUDAFEvaluator.Mode.PARTIAL1, doubleOI);
    eval2.init(GenericUDAFEvaluator.Mode.PARTIAL1, doubleOI);

    GenericUDAFEvaluator.AggregationBuffer buffer1 = eval1.getNewAggregationBuffer();
    eval1.iterate(buffer1, new Object[] {Arrays.asList(1d, 2d, 3d)});
    eval1.iterate(buffer1, new Object[] {Arrays.asList(4d, 5d, 6d)});
    eval1.iterate(buffer1, new Object[] {Arrays.asList(7d, 8d, 9d)});
    Object object1 = eval1.terminatePartial(buffer1);

    GenericUDAFEvaluator.AggregationBuffer buffer2 = eval2.getNewAggregationBuffer();
    eval2.iterate(buffer2, new Object[] {Arrays.asList(10d, 11d, 12d)});
    eval2.iterate(buffer2, new Object[] {Arrays.asList(4d, 5d, 6d)});
    eval2.iterate(buffer2, new Object[] {Arrays.asList(14d, 15d, 16d)});
    eval2.iterate(buffer2, new Object[] {Arrays.asList(100d, 101d, 102d)});
    Object object2 = eval2.terminatePartial(buffer2);

    eval2.init(GenericUDAFEvaluator.Mode.FINAL, doubleOI);
    GenericUDAFEvaluator.AggregationBuffer buffer3 = eval2.getNewAggregationBuffer();
    eval2.merge(buffer3, object1);
    eval2.merge(buffer3, object2);

    Object result = eval2.terminate(buffer3);
    List<Double> expected = getResults();
    assertEquals(expected, result);
  }
}
