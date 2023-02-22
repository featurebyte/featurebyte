package com.featurebyte.hive.udf;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;

import java.util.Map;

@Description(name = "object_delete",
  value = "_FUNC_(counts) "
    + "- remove a key from count dictionary"
)
public class ObjectDelete extends CountDictUDF {

  final private transient PrimitiveObjectInspector.PrimitiveCategory[] stringInputTypes = new PrimitiveObjectInspector.PrimitiveCategory[3];
  final private transient ObjectInspectorConverters.Converter[] stringConverters = new ObjectInspectorConverters.Converter[1];

  @Override
  public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
    checkArgsSize(arguments, 2, 2);
    if (isNullOI(arguments[0])) {
      return nullOI;
    }

    // Map
    checkTypesAndInitialize(arguments);

    // Key to delete
    ObjectInspector[] args = {arguments[1]};
    checkArgPrimitive(args, 0);
    obtainStringConverter(args, 0, stringInputTypes, stringConverters);

    return inputMapOI;
  }

  @Override
  public Object evaluate(DeferredObject[] arguments) throws HiveException {
    if (arguments[0].get() == null) {
      return null;
    }
    String keyToDelete = stringConverters[0].convert(arguments[1].get()).toString();
    Map<String, Object> counts = (Map<String, Object>) inputMapOI.getMap(arguments[0].get());
    counts.remove(keyToDelete);
    return counts;
  }

  @Override
  public String getDisplayString(String[] children) {
    return "OBJECT_DELETE";
  }
}
