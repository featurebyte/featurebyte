package com.featurebyte.hive.udf;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;

public abstract class CountDictSingleStringArgumentUDF extends CountDictUDF {

  protected final transient PrimitiveObjectInspector.PrimitiveCategory[] stringInputTypes =
      new PrimitiveObjectInspector.PrimitiveCategory[1];
  protected final transient ObjectInspectorConverters.Converter[] stringConverters =
      new ObjectInspectorConverters.Converter[1];

  @Override
  protected void checkTypesAndInitialize(ObjectInspector[] arguments) throws UDFArgumentException {
    // Map
    super.checkTypesAndInitialize(arguments);

    // Key of interest
    ObjectInspector[] args = {arguments[1]};
    checkArgPrimitive(args, 0);
    obtainStringConverter(args, 0, stringInputTypes, stringConverters);
  }

  protected String getStringArgument(DeferredObject[] arguments) throws HiveException {
    return stringConverters[0].convert(arguments[1].get()).toString();
  }
}
