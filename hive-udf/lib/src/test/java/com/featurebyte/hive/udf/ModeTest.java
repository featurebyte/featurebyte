package com.featurebyte.hive.udf;

import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class ModeTest {
  Mode udaf = new Mode();

  @Test
  public void testMode() throws HiveException {
    GenericUDAFEvaluator eval1 = udaf.getEvaluator(
      new TypeInfo[]{TypeInfoFactory.stringTypeInfo});
    GenericUDAFEvaluator eval2 = udaf.getEvaluator(
      new TypeInfo[]{TypeInfoFactory.stringTypeInfo});

    ObjectInspector poi1 = eval1.init(GenericUDAFEvaluator.Mode.PARTIAL1,
      new ObjectInspector[]{PrimitiveObjectInspectorFactory.javaStringObjectInspector});
    eval2.init(GenericUDAFEvaluator.Mode.PARTIAL1,
      new ObjectInspector[]{PrimitiveObjectInspectorFactory.javaStringObjectInspector});

    GenericUDAFEvaluator.AggregationBuffer buffer1 = eval1.getNewAggregationBuffer();
    eval1.iterate(buffer1, new Object[]{"apple"});
    eval1.iterate(buffer1, new Object[]{"orange"});
    eval1.iterate(buffer1, new Object[]{"orange"});
    Object object1 = eval1.terminatePartial(buffer1);

    GenericUDAFEvaluator.AggregationBuffer buffer2 = eval2.getNewAggregationBuffer();
    eval2.iterate(buffer2, new Object[]{"grape"});
    eval2.iterate(buffer2, new Object[]{"grape"});
    eval2.iterate(buffer2, new Object[]{"grape"});
    eval2.iterate(buffer2, new Object[]{"apple"});
    Object object2 = eval2.terminatePartial(buffer2);

    eval2.init(GenericUDAFEvaluator.Mode.FINAL, new ObjectInspector[]{poi1});
    GenericUDAFEvaluator.AggregationBuffer buffer3 = eval2.getNewAggregationBuffer();
    eval2.merge(buffer3, object1);
    eval2.merge(buffer3, object2);

    Object result = eval2.terminate(buffer3);
    assertEquals("grape", result);
  }
}
