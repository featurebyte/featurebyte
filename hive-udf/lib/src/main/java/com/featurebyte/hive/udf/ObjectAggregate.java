package com.featurebyte.hive.udf;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.udf.generic.AbstractGenericUDAFResolver;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;

import static org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category.PRIMITIVE;

@Description(name = "object_agg", value = "_FUNC_(x) - Aggregate objects")
public class ObjectAggregate extends AbstractGenericUDAFResolver {

  public ObjectAggregate() {
  }

  @Override
  public GenericUDAFEvaluator getEvaluator(TypeInfo[] parameters)
    throws SemanticException {
    if (parameters.length != 2) {
      throw new UDFArgumentTypeException(parameters.length - 1,
        "Exactly two arguments are expected.");
    }

    if (parameters[0].getCategory() != PRIMITIVE) {
      throw new UDFArgumentTypeException(0, "Parameter 1 must be a primitive");
    }

    if (parameters[1].getCategory() != PRIMITIVE) {
      throw new UDFArgumentTypeException(0, "Parameter 2 must be a primitive");
    }

    switch (((PrimitiveTypeInfo) parameters[0]).getPrimitiveCategory()) {
      case STRING:
      case VARCHAR:
        return new ObjectAggregatorEvaluator();
      default:
        throw new UDFArgumentTypeException(0, "Only string is accepted for parameter 1");
    }
  }
}
