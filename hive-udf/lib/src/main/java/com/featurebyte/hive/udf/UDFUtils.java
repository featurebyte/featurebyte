package com.featurebyte.hive.udf;

import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.WritableVoidObjectInspector;

public class UDFUtils {

  protected static boolean isNullOI(ObjectInspector objectInspector) {
    return objectInspector instanceof WritableVoidObjectInspector;
  }

  protected static final WritableVoidObjectInspector nullOI =
      PrimitiveObjectInspectorFactory.writableVoidObjectInspector;
}
