package com.featurebyte.hive.udf;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.junit.jupiter.api.Test;

public class VectorCosineSimilarityTest {

  private final ObjectInspector listValueOI =
      ObjectInspectorFactory.getStandardListObjectInspector(
          PrimitiveObjectInspectorFactory.javaDoubleObjectInspector);

  private static GenericUDF.DeferredJavaObject wrapInDeferredJavaObject(Object obj) {
    return new GenericUDF.DeferredJavaObject(obj);
  }

  @Test
  public void testVectorCosineSimilarity() throws HiveException {
    List<Integer> inputOne = new ArrayList<>(Arrays.asList(1, 2, 3));
    List<Double> inputTwo = new ArrayList<>(Arrays.asList(1.0, 2.0, 3.0));

    VectorCosineSimilarityV1 udf = new VectorCosineSimilarityV1();
    ObjectInspector[] arguments = {listValueOI, listValueOI};
    udf.initialize(arguments);
    GenericUDF.DeferredObject[] args = {
      wrapInDeferredJavaObject(inputOne), wrapInDeferredJavaObject(inputTwo)
    };
    Double output = (Double) udf.evaluate(args);
    assertEquals(output, 1.0);
  }
}
