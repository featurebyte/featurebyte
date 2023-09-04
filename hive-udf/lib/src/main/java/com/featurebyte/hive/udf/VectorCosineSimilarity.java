package com.featurebyte.hive.udf;

import static com.featurebyte.hive.udf.UDFUtils.isNullOI;
import static com.featurebyte.hive.udf.UDFUtils.nullOI;
import static org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category.LIST;

import java.util.List;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.*;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

@Description(
    name = "F_VECTOR_COSINE_SIMILARITY",
    value = "_FUNC_(counts) - compute cosine similarity between two arrays")
public class VectorCosineSimilarity extends GenericUDF {

  private transient ListObjectInspector firstListOI;
  private transient ListObjectInspector secondListOI;

  @Override
  public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
    checkArgsSize(arguments, 2, 2);
    if (isNullOI(arguments[0]) || isNullOI(arguments[1])) {
      return nullOI;
    }

    // Check that the arguments are of list type
    ObjectInspector firstParameter = arguments[0];
    if (firstParameter.getCategory() != LIST) {
      throw new UDFArgumentTypeException(0, "Parameter 1 must be a list");
    }
    ObjectInspector secondParameter = arguments[1];
    if (secondParameter.getCategory() != LIST) {
      throw new UDFArgumentTypeException(1, "Parameter 2 must be a list");
    }

    // Assign object inspectors to local variables
    firstListOI = (StandardListObjectInspector) firstParameter;
    secondListOI = (StandardListObjectInspector) secondParameter;

    // We always return a double as the result of the cosine similarity evaluation.
    return PrimitiveObjectInspectorFactory.javaDoubleObjectInspector;
  }

  /** Calculate the dot product between two lists. */
  private static double dotProduct(List<Double> listOne, List<Double> listTwo) {
    // Find length of shorter list.
    int shorterLength = Math.min(listOne.size(), listTwo.size());
    // Calculate dot product between shorter list size
    double sum = 0.0;
    for (int i = 0; i < shorterLength; i++) {
      Double valueOne = listOne.get(i);
      Double valueTwo = listTwo.get(i);
      sum += valueOne * valueTwo;
    }
    return sum;
  }

  private static double euclideanNorm(List<Double> numList) {
    double sum = 0.0;
    for (Double num : numList) {
      sum += Math.pow(num, 2);
    }
    return Math.sqrt(sum);
  }

  @Override
  public Object evaluate(DeferredObject[] arguments) throws HiveException {
    if (arguments[0].get() == null || arguments[1].get() == null) {
      return null;
    }

    // Convert parameters to List<Double>
    List<Double> listOne = (List<Double>) firstListOI.getList(arguments[0].get());
    List<Double> listTwo = (List<Double>) secondListOI.getList(arguments[1].get());
    // Check if lists are empty
    if (listOne.isEmpty() || listTwo.isEmpty()) {
      return 0.0;
    }

    double dotProduct = dotProduct(listOne, listTwo);
    double norm = euclideanNorm(listOne);
    double normOther = euclideanNorm(listTwo);

    return dotProduct / (norm * normOther);
  }

  @Override
  public String getDisplayString(String[] children) {
    return "F_VECTOR_COSINE_SIMILARITY";
  }
}
