package com.featurebyte.hive.udf;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.MapObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.ObjectWritable;
import org.apache.hadoop.io.Text;

import java.util.Map;
import java.util.Objects;

import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils.PrimitiveGrouping.NUMERIC_GROUP;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils.PrimitiveGrouping.STRING_GROUP;

@Description(name = "object_delete",
  value = "_FUNC_(counts) "
    + "- remove a key from the count dictionary"
)
public class ObjectDelete extends GenericUDF {
//  final private MapWritable output = new MapWritable();

  private transient MapObjectInspector inputMapOI;
  private transient PrimitiveObjectInspector inputStringOI;
  final private transient PrimitiveObjectInspector.PrimitiveCategory[] inputTypes = new PrimitiveObjectInspector.PrimitiveCategory[2];
  final private transient ObjectInspectorConverters.Converter[] converters = new ObjectInspectorConverters.Converter[2];

  @Override
  public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
    checkArgsSize(arguments, 2, 2);
    if (!(arguments[0] instanceof MapObjectInspector)) {
      throw new UDFArgumentTypeException(0, "Parameter 1 must be a Map");
    }
    inputMapOI = (MapObjectInspector) arguments[0];
    ObjectInspector[] map_args = {
      inputMapOI.getMapKeyObjectInspector(),
      inputMapOI.getMapValueObjectInspector()
    };

    try {
      checkArgPrimitive(map_args,0);
      checkArgGroups(map_args, 0, inputTypes, STRING_GROUP);
    } catch (UDFArgumentException e) {
      throw new UDFArgumentTypeException(0, "Map key must be a string");
    }

    try {
      checkArgPrimitive(map_args,1);
      checkArgGroups(map_args, 1, inputTypes, NUMERIC_GROUP);
      obtainDoubleConverter(map_args,1, inputTypes, converters);
    } catch (UDFArgumentException e) {
      throw new UDFArgumentTypeException(0, "Map value must be numeric");
    }

//    return PrimitiveObjectInspectorFactory.writableStringObjectInspector;
    return inputMapOI;
  }

  @Override
  public Object evaluate(DeferredObject[] arguments) throws HiveException {
    if (arguments[0] == null) {
      return null;
    }
    String key_to_delete = PrimitiveObjectInspectorFactory.javaStringObjectInspector.getPrimitiveWritableObject(arguments[1].get()).toString();
    Map<String, Object> counts = (Map<String, Object>) inputMapOI.getMap(arguments[0].get());
    /*
    for (Map.Entry<String, Object> entry : counts.entrySet()) {
      if (!Objects.equals(entry.getKey(), key_to_delete)) {
        Text key_writable = new Text(entry.getKey());
        ObjectWritable value_writable = new ObjectWritable(entry.getValue());
        output.put(key_writable, value_writable);
      }
    }
    return output;
    */
    counts.remove(key_to_delete);
    return counts;
  }

  @Override
  public String getDisplayString(String[] children) {
    return "OBJECT_DELETE";
  }
}
