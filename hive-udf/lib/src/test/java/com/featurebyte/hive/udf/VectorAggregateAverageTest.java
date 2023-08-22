package com.featurebyte.hive.udf;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Arrays;
import java.util.List;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.ql.udf.generic.SimpleGenericUDAFParameterInfo;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.junit.jupiter.api.Test;

public class VectorAggregateAverageTest {

  @Test
  public void testVectorAggregate() throws HiveException {
    ObjectInspector[] doubleOI =
        new ObjectInspector[] {
          ObjectInspectorFactory.getStandardListObjectInspector(
              PrimitiveObjectInspectorFactory.javaDoubleObjectInspector),
          PrimitiveObjectInspectorFactory.javaLongObjectInspector,
        };
    VectorAggregateAverage udaf = new VectorAggregateAverage();
    SimpleGenericUDAFParameterInfo info =
        new SimpleGenericUDAFParameterInfo(doubleOI, false, false, false);
    GenericUDAFEvaluator eval1 = udaf.getEvaluator(info);
    GenericUDAFEvaluator eval2 = udaf.getEvaluator(info);

    eval1.init(GenericUDAFEvaluator.Mode.PARTIAL1, doubleOI);
    eval2.init(GenericUDAFEvaluator.Mode.PARTIAL1, doubleOI);

    GenericUDAFEvaluator.AggregationBuffer buffer1 = eval1.getNewAggregationBuffer();
    eval1.iterate(buffer1, new Object[] {Arrays.asList(1d, 2d, 3d), 1L});
    eval1.iterate(buffer1, new Object[] {Arrays.asList(2d, 4d, 6d), 2L});
    eval1.iterate(buffer1, new Object[] {Arrays.asList(3d, 6d, 9d), 3L});
    Object object1 = eval1.terminatePartial(buffer1);

    GenericUDAFEvaluator.AggregationBuffer buffer2 = eval2.getNewAggregationBuffer();
    eval2.iterate(buffer2, new Object[] {Arrays.asList(1d, 2d, 3d), 1L});
    eval2.iterate(buffer2, new Object[] {Arrays.asList(1d, 2d, 3d), 1L});
    eval2.iterate(buffer2, new Object[] {Arrays.asList(1d, 2d, 3d), 1L});
    eval2.iterate(buffer2, new Object[] {Arrays.asList(4d, 8d, 12d), 4L});
    Object object2 = eval2.terminatePartial(buffer2);

    eval2.init(GenericUDAFEvaluator.Mode.FINAL, doubleOI);
    GenericUDAFEvaluator.AggregationBuffer buffer3 = eval2.getNewAggregationBuffer();
    eval2.merge(buffer3, object1);
    eval2.merge(buffer3, object2);

    Object result = eval2.terminate(buffer3);
    List<Double> expected = Arrays.asList(1d, 2d, 3d);
    List<Double> actual = Arrays.asList((Double[]) result);
    assertEquals(expected, actual);
  }
}
