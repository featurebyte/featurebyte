package com.featurebyte.hive.udf;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.junit.jupiter.api.Test;

public class ObjectAggregateTest {
  ObjectAggregate udaf = new ObjectAggregate();

  @Test
  public void testObjectAggregate() throws HiveException {
    GenericUDAFEvaluator eval1 =
        udaf.getEvaluator(
            new TypeInfo[] {TypeInfoFactory.stringTypeInfo, TypeInfoFactory.doubleTypeInfo});
    GenericUDAFEvaluator eval2 =
        udaf.getEvaluator(
            new TypeInfo[] {TypeInfoFactory.stringTypeInfo, TypeInfoFactory.doubleTypeInfo});

    ObjectInspector poi1 =
        eval1.init(
            GenericUDAFEvaluator.Mode.PARTIAL1,
            new ObjectInspector[] {
              PrimitiveObjectInspectorFactory.javaStringObjectInspector,
              PrimitiveObjectInspectorFactory.javaDoubleObjectInspector
            });
    eval2.init(
        GenericUDAFEvaluator.Mode.PARTIAL1,
        new ObjectInspector[] {
          PrimitiveObjectInspectorFactory.javaStringObjectInspector,
          PrimitiveObjectInspectorFactory.javaDoubleObjectInspector
        });

    GenericUDAFEvaluator.AggregationBuffer buffer1 = eval1.getNewAggregationBuffer();
    eval1.iterate(buffer1, new Object[] {"apple", 200d});
    eval1.iterate(buffer1, new Object[] {"orange", 210d});
    eval1.iterate(buffer1, new Object[] {"pear", 220d});
    Object object1 = eval1.terminatePartial(buffer1);

    GenericUDAFEvaluator.AggregationBuffer buffer2 = eval2.getNewAggregationBuffer();
    eval2.iterate(buffer2, new Object[] {"grape", 230d});
    eval2.iterate(buffer2, new Object[] {"guava", 240d});
    eval2.iterate(buffer2, new Object[] {"banana", 250d});
    eval2.iterate(buffer2, new Object[] {"strawberry", 260d});
    Object object2 = eval2.terminatePartial(buffer2);

    eval2.init(GenericUDAFEvaluator.Mode.FINAL, new ObjectInspector[] {poi1});
    GenericUDAFEvaluator.AggregationBuffer buffer3 = eval2.getNewAggregationBuffer();
    eval2.merge(buffer3, object1);
    eval2.merge(buffer3, object2);

    Object result = eval2.terminate(buffer3);
    Map<String, Double> expected = new HashMap<>();
    expected.put("apple", 200d);
    expected.put("orange", 210d);
    expected.put("pear", 220d);
    expected.put("grape", 230d);
    expected.put("guava", 240d);
    expected.put("banana", 250d);
    expected.put("strawberry", 260d);
    assertEquals(expected, result);
  }
}
