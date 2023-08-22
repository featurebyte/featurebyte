package com.featurebyte.hive.udf;

import static org.junit.jupiter.api.Assertions.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.ql.udf.generic.SimpleGenericUDAFParameterInfo;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.junit.jupiter.api.Test;

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
      fail(
          "iterate should throw an assertion error as the size of the array passed in is different");
    } catch (RuntimeException ignored) {
    }
  }

  public void vectorAggregateTestHelper(
      ObjectInspector listElementOI,
      List<Object> evalOneItems,
      List<Object> evalTwoItems,
      List<Double> expectedResults)
      throws HiveException {
    ObjectInspector[] listOI =
        new ObjectInspector[] {
          ObjectInspectorFactory.getStandardListObjectInspector(listElementOI),
        };
    BaseVectorAggregate udaf = getAggregator();
    SimpleGenericUDAFParameterInfo info =
        new SimpleGenericUDAFParameterInfo(listOI, false, false, false);
    GenericUDAFEvaluator eval1 = udaf.getEvaluator(info);
    GenericUDAFEvaluator eval2 = udaf.getEvaluator(info);

    eval1.init(GenericUDAFEvaluator.Mode.PARTIAL1, listOI);
    eval2.init(GenericUDAFEvaluator.Mode.PARTIAL1, listOI);

    GenericUDAFEvaluator.AggregationBuffer buffer1 = eval1.getNewAggregationBuffer();
    for (Object item : evalOneItems) {
      eval1.iterate(buffer1, new Object[] {item});
    }
    Object object1 = eval1.terminatePartial(buffer1);

    GenericUDAFEvaluator.AggregationBuffer buffer2 = eval2.getNewAggregationBuffer();
    for (Object item : evalTwoItems) {
      eval2.iterate(buffer2, new Object[] {item});
    }
    Object object2 = eval2.terminatePartial(buffer2);

    eval2.init(GenericUDAFEvaluator.Mode.FINAL, listOI);
    GenericUDAFEvaluator.AggregationBuffer buffer3 = eval2.getNewAggregationBuffer();
    eval2.merge(buffer3, object1);
    eval2.merge(buffer3, object2);

    Object result = eval2.terminate(buffer3);
    assertEquals(expectedResults, result);
  }

  @Test
  public void testVectorAggregateDoubleInputs() throws HiveException {
    List<Object> evalOneList = new ArrayList<>();
    evalOneList.add(Arrays.asList(1d, 2d, 3d));
    evalOneList.add(Arrays.asList(4d, 5d, 6d));
    evalOneList.add(Arrays.asList(7d, 8d, 9d));

    List<Object> evalTwoList = new ArrayList<>();
    evalTwoList.add(Arrays.asList(10d, 11d, 12d));
    evalTwoList.add(Arrays.asList(4d, 5d, 6d));
    evalTwoList.add(Arrays.asList(14d, 15d, 16d));
    evalTwoList.add(Arrays.asList(100d, 101d, 102d));

    vectorAggregateTestHelper(
        PrimitiveObjectInspectorFactory.javaDoubleObjectInspector,
        evalOneList,
        evalTwoList,
        getResults());
  }

  @Test
  public void testVectorAggregateLongInputs() throws HiveException {
    List<Object> evalOneList = new ArrayList<>();
    evalOneList.add(Arrays.asList(1L, 2L, 3L));
    evalOneList.add(Arrays.asList(4L, 5L, 6L));
    evalOneList.add(Arrays.asList(7L, 8L, 9L));

    List<Object> evalTwoList = new ArrayList<>();
    evalTwoList.add(Arrays.asList(10L, 11L, 12L));
    evalTwoList.add(Arrays.asList(4L, 5L, 6L));
    evalTwoList.add(Arrays.asList(14L, 15L, 16L));
    evalTwoList.add(Arrays.asList(100L, 101L, 102L));

    vectorAggregateTestHelper(
        PrimitiveObjectInspectorFactory.javaLongObjectInspector,
        evalOneList,
        evalTwoList,
        getResults());
  }

  @Test
  public void testVectorAggregateIntInputs() throws HiveException {
    List<Object> evalOneList = new ArrayList<>();
    evalOneList.add(Arrays.asList(1, 2, 3));
    evalOneList.add(Arrays.asList(4, 5, 6));
    evalOneList.add(Arrays.asList(7, 8, 9));

    List<Object> evalTwoList = new ArrayList<>();
    evalTwoList.add(Arrays.asList(10, 11, 12));
    evalTwoList.add(Arrays.asList(4, 5, 6));
    evalTwoList.add(Arrays.asList(14, 15, 16));
    evalTwoList.add(Arrays.asList(100, 101, 102));

    vectorAggregateTestHelper(
        PrimitiveObjectInspectorFactory.javaIntObjectInspector,
        evalOneList,
        evalTwoList,
        getResults());
  }
}
