package com.featurebyte.hive.udf;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.*;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;

import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils.PrimitiveGrouping.NUMERIC_GROUP;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils.PrimitiveGrouping.STRING_GROUP;

public abstract class CountDictUDF extends GenericUDF {

  public static void checkIsMap(ObjectInspector[] arguments, int i) throws UDFArgumentTypeException {
    if (!(arguments[i] instanceof MapObjectInspector)) {
      throw new UDFArgumentTypeException(i, "Parameter must be a Map");
    }
  }

  public MapObjectInspector checkTypesAndConstructMapOI(
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
      throw new UDFArgumentTypeException(0, "Map value must be numeric");
    }

    return inputMapOI;
  }

}
