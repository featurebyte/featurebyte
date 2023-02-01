package com.featurebyte.hive.udf;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.udf.generic.AbstractGenericUDAFResolver;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;

import static org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category.PRIMITIVE;

@Description(name = "mode", value = "_FUNC_(x) - Get most common value")
public class Mode extends AbstractGenericUDAFResolver {

  public Mode() {
  }

  @Override
  public GenericUDAFEvaluator getEvaluator(TypeInfo[] parameters)
    throws SemanticException {
    if (parameters.length != 1) {
      throw new UDFArgumentTypeException(parameters.length - 1,
        "Exactly one arguments is expected.");
    }

    if (parameters[0].getCategory() != PRIMITIVE) {
      throw new UDFArgumentTypeException(0, "Parameter 1 must be a primitive");
    }
    return new ModeEvaluator();
  }
}
