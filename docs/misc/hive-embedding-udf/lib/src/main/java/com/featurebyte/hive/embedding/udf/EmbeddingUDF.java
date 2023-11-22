package com.featurebyte.hive.embedding.udf;

import com.auth0.jwt.JWT;
import com.auth0.jwt.JWTCreator;
import com.auth0.jwt.algorithms.Algorithm;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.auth.oauth2.ServiceAccountCredentials;
import java.io.FileInputStream;
import java.net.URI;
import java.security.interfaces.RSAPrivateKey;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;
import org.apache.hadoop.io.*;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.apache.http.HttpEntity;
import org.apache.http.client.config.RequestConfig;


@Description(name = "F_SBERT_EMBEDDING", value = "_FUNC_(x) - compute embeddings for a vector")
public class EmbeddingUDF extends GenericUDF {
  private StringObjectInspector input;

  /**
   * Generate JWT token.
   *
   * @param     saKeyfile      path to keyfile
   * @param     saEmail        client email
   * @param     audience       audience
   * @param     expieryLength  token expiery seconds
   * @return                   JWT token
   */
  public static String generateJwt(
      final String saKeyfile, final String saEmail, final String audience, final int expieryLength) {
    Date now = new Date();
    Date expTime = new Date(System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(expieryLength));

    JWTCreator.Builder token =
        JWT.create()
            .withIssuedAt(now)
            .withExpiresAt(expTime)
            .withIssuer(saEmail)
            .withAudience(audience)
            .withSubject(saEmail)
            .withClaim("email", saEmail);

    try {
      FileInputStream stream = new FileInputStream(saKeyfile);
      ServiceAccountCredentials cred = ServiceAccountCredentials.fromStream(stream);
      RSAPrivateKey key = (RSAPrivateKey) cred.getPrivateKey();
      Algorithm algorithm = Algorithm.RSA256(null, key);
      return token.sign(algorithm);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Call embedding model deployed as a GCP Cloud Function.
   * The method expects following ENV variables:
   * - ENDPOINT_URL: Cloud function URL
   * - SA_KEYFILE_PATH: Path to the keyfile from the GCP Service Account
   * - SA_EMAIL: Client email
   * - SA_AUDIENCE: Google audience, AKA value of Managed Service from API Gateway details.
   *
   * @param    value   value to compute embedding for
   * @return           embedding - list of doubles
   */
  public Object evaluateGCPFunction(String value) throws Exception {
    Object val[] = new Object[] {0, value};
    List<List<Object>> list = new ArrayList<List<Object>>();
    list.add(Arrays.asList(val));
    Map<String, List<List<Object>>> data = new HashMap<String, List<List<Object>>>();
    data.put("data", list);

    ObjectMapper mapper = new ObjectMapper();
    String body = mapper.writeValueAsString(data);

    String url = System.getenv("ENDPOINT_URL");
    String keyfile = System.getenv("SA_KEYFILE_PATH");
    String email = System.getenv("SA_EMAIL");
    String audience = System.getenv("SA_AUDIENCE");

    String token = EmbeddingUDF.generateJwt(keyfile, email, audience, 3600);

    RequestConfig requestConfig = RequestConfig.custom()
      .setConnectionRequestTimeout(600 * 1000)
      .setConnectTimeout(600 * 1000)
      .setSocketTimeout(600 * 1000)
      .build();
    HttpPost post = new HttpPost(url);
    StringEntity stringEntity = new StringEntity(body);
    post.setEntity(stringEntity);
    post.setHeader("Accept", "application/json");
    post.setHeader("Content-type", "application/json");
    post.setHeader("Authorization", "Bearer " + token);
    post.setConfig(requestConfig);

    int retry = 0;
    String response = null;
    Exception lastExc = null;
    while (response == null && retry < 10) {
      try (
        CloseableHttpClient httpClient = HttpClients.createDefault();
        CloseableHttpResponse resp = httpClient.execute(post);
      ) {
        response = EntityUtils.toString(resp.getEntity());
      } catch (Exception e) {
        lastExc = e;
        response = null;
        retry++;

        TimeUnit.SECONDS.sleep(2 * retry);
      }
    }

    if (response == null) {
      throw new RuntimeException("No response in " + retry + " retries", lastExc);
    }

    Map<String, List> map = mapper.readValue(response, HashMap.class);
    List element = (List) map.get("data").get(0);
    List array = (List) element.get(1);
    return (List<Double>) array;
  }

  /**
   * Call embedding model deployed via Databricks MLFLow.
   * The method expects following ENV variables:
   * - ENDPOINT_URL: MLFlow serving URL
   * - DB_TOKEN: Databricks access token.
   *
   * @param    value   value to compute embedding for
   * @return           embedding - list of doubles
   */
  public Object evaluateDatabricksMLFlow(String value) throws Exception {
    Map<String, List<String>> text = new HashMap<String, List<String>>();
    text.put("text", Arrays.asList(value));
    Map<String, Map<String, List<String>>> data = new HashMap<String, Map<String, List<String>>>();
    data.put("inputs", text);

    ObjectMapper mapper = new ObjectMapper();
    String body = mapper.writeValueAsString(data);

    String url = System.getenv("ENDPOINT_URL");
    String token = System.getenv("DB_TOKEN");

    RequestConfig requestConfig = RequestConfig.custom()
      .setConnectionRequestTimeout(600 * 1000)
      .setConnectTimeout(600 * 1000)
      .setSocketTimeout(600 * 1000)
      .build();
    HttpPost post = new HttpPost(url);
    StringEntity stringEntity = new StringEntity(body);
    post.setEntity(stringEntity);
    post.setHeader("Content-type", "application/json");
    post.setHeader("Authorization", "Bearer " + token);
    post.setConfig(requestConfig);

    int retry = 0;
    String response = null;
    Exception lastExc = null;
    while (response == null && retry < 10) {
      try (
        CloseableHttpClient httpClient = HttpClients.createDefault();
        CloseableHttpResponse resp = httpClient.execute(post);
      ) {
        response = EntityUtils.toString(resp.getEntity());
      } catch (Exception e) {
        lastExc = e;
        response = null;
        retry++;

        TimeUnit.SECONDS.sleep(2 * retry);
      }
    }

    if (response == null) {
      throw new RuntimeException("No response in " + retry + " retries", lastExc);
    }

    Map<String, List> map = mapper.readValue(response, HashMap.class);
    List<List<Double>> preds = (List<List<Double>>) map.get("predictions");
    List<Double> embeddings = preds.get(0);
    return embeddings;
  }

  /**
   * Call embedding model deployed via custom TorchService service.
   * Doesn't expect any authentication at this point.
   * The method expects following ENV variables:
   * - ENDPOINT_URL: TorchService service URL
   *
   * @param    value   value to compute embedding for
   * @return           embedding - list of doubles
   */
  public Object evaluateTorchServe(String value) throws Exception {
    Map<String, String> data = new HashMap<String, String>();
    data.put("text", value);

    ObjectMapper mapper = new ObjectMapper();
    String body = mapper.writeValueAsString(data);

    String url = System.getenv("ENDPOINT_URL");
    // String url = "http://host.docker.internal:8080/predictions/transformer";

    RequestConfig requestConfig = RequestConfig.custom()
      .setConnectionRequestTimeout(600 * 1000)
      .setConnectTimeout(600 * 1000)
      .setSocketTimeout(600 * 1000)
      .build();
    HttpPost post = new HttpPost(url);
    StringEntity stringEntity = new StringEntity(body);
    post.setEntity(stringEntity);
    post.setHeader("Accept", "application/json");
    post.setHeader("Content-type", "application/json");
    post.setConfig(requestConfig);

    int retry = 0;
    String response = null;
    Exception lastExc = null;
    while (response == null && retry < 10) {
      try (
        CloseableHttpClient httpClient = HttpClients.createDefault();
        CloseableHttpResponse resp = httpClient.execute(post);
      ) {
        response = EntityUtils.toString(resp.getEntity());
      } catch (Exception e) {
        lastExc = e;
        response = null;
        retry++;

        TimeUnit.SECONDS.sleep(2 * retry);
      }
    }

    if (response == null) {
      throw new RuntimeException("No response in " + retry + " retries", lastExc);
    }

    Map<String, List> map = mapper.readValue(response, HashMap.class);
    List array = (List) map.get("result");
    return (List<Double>) array;
  }

  public Object evaluate(DeferredObject[] arguments) throws HiveException {
    if (input == null || arguments.length != 1 || arguments[0].get() == null) {
      return null;
    }
    String value = input.getPrimitiveJavaObject(arguments[0].get()).toString();

    try {
      String apiType = System.getenv("API_TYPE");
      if (apiType.equals("gcp")) {
        return this.evaluateGCPFunction(value);
      } else if (apiType.equals("mlflow")) {
        return this.evaluateDatabricksMLFlow(value);
      } else {
        return this.evaluateTorchServe(value);
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public String getDisplayString(String[] arguments) {
    return "F_SBERT_EMBEDDING";
  }

  public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
    if (arguments.length != 1) {
      throw new UDFArgumentException("input must have length 1");
    }

    ObjectInspector input = arguments[0];
    this.input = (StringObjectInspector) input;

    return ObjectInspectorFactory.getStandardListObjectInspector(
        PrimitiveObjectInspectorFactory.javaDoubleObjectInspector);
  }
}
