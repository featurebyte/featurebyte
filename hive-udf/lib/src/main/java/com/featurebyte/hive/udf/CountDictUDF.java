package com.featurebyte.hive.udf;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.*;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.WritableVoidObjectInspector;

import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils.PrimitiveGrouping.NUMERIC_GROUP;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils.PrimitiveGrouping.STRING_GROUP;

public abstract class CountDictUDF extends GenericUDF {

  protected transient MapObjectInspector inputMapOI;
  final protected transient PrimitiveCategory[] inputTypes = new PrimitiveCategory[2];
  final protected transient ObjectInspectorConverters.Converter[] converters = new ObjectInspectorConverters.Converter[2];

  protected static final WritableVoidObjectInspector nullOI = PrimitiveObjectInspectorFactory.writableVoidObjectInspector;

  protected static void checkIsMap(ObjectInspector[] arguments, int i) throws UDFArgumentTypeException {
    if (!(arguments[i] instanceof MapObjectInspector)) {
      throw new UDFArgumentTypeException(i, "Parameter must be a Map");
    }
  }

  protected static boolean isNullOI(ObjectInspector objectInspector) {
    return objectInspector instanceof WritableVoidObjectInspector;
  }

  protected void checkTypesAndInitialize(ObjectInspector[] arguments) throws UDFArgumentException {
    checkIsMap(arguments, 0);
    inputMapOI = checkTypesAndConstructMapOI(arguments[0], inputTypes, converters);
  }

  protected MapObjectInspector checkTypesAndConstructMapOI(
    ObjectInspector mapOI,
    PrimitiveCategory[] inputTypes,
    ObjectInspectorConverters.Converter[] converters
  ) throws UDFArgumentTypeException {

    MapObjectInspector inputMapOI = (MapObjectInspector) mapOI;
    ObjectInspector[] map_args = {
      inputMapOI.getMapKeyObjectInspector(),
      inputMapOI.getMapValueObjectInspector()
    };

    try {
      checkArgPrimitive(map_args, 0);
      checkArgGroups(map_args, 0, inputTypes, STRING_GROUP);
      obtainDoubleConverter(map_args, 0, inputTypes, converters);
    } catch (UDFArgumentException e) {
      throw new UDFArgumentTypeException(0, "Map key must be a string");
    }

    try {
      checkArgPrimitive(map_args,1);
      checkArgGroups(map_args, 1, inputTypes, NUMERIC_GROUP);
      obtainDoubleConverter(map_args, 1, inputTypes, converters);
    } catch (UDFArgumentException e) {
      throw new UDFArgumentTypeException(1, "Map value must be numeric");
    }

    return inputMapOI;
  }

}
