package com.featurebyte.hive.udf;

import com.auth0.jwt.JWT;
import com.auth0.jwt.JWTCreator;
import com.auth0.jwt.algorithms.Algorithm;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.auth.oauth2.ServiceAccountCredentials;
import java.io.FileInputStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
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

@Description(name = "F_SBERT_EMBEDDING", value = "_FUNC_(counts) - compute embeddings for a vector")
public class EmbeddingUDF extends GenericUDF {
  private StringObjectInspector input;

  public static String generateJwt(
      final String saKeyfile, final String saEmail, final String audience, final int expiryLength) {
    Date now = new Date();
    Date expTime = new Date(System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(expiryLength));

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

  public Object evaluate(DeferredObject[] arguments) throws HiveException {
    if (input == null || arguments.length != 1 || arguments[0].get() == null) {
      return null;
    }
    String value = input.getPrimitiveJavaObject(arguments[0].get()).toString();

    Object val[] = new Object[] {0, value};
    List<List<Object>> list = new ArrayList<List<Object>>();
    list.add(Arrays.asList(val));
    Map<String, List<List<Object>>> data = new HashMap<String, List<List<Object>>>();
    data.put("data", list);

    ObjectMapper mapper = new ObjectMapper();
    String body = null;
    try {
      body = mapper.writeValueAsString(data);
    } catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }

    String url = System.getenv("FUNCTION_URL");
    String keyfile = System.getenv("SA_KEYFILE_PATH");
    String email = System.getenv("SA_EMAIL");
    String audience = System.getenv("SA_AUDIENCE");

    String token = EmbeddingUDF.generateJwt(keyfile, email, audience, 3600);

    HttpClient client = HttpClient.newHttpClient();
    HttpRequest request =
        HttpRequest.newBuilder(URI.create(url))
            .timeout(Duration.ofSeconds(600))
            .header("accept", "application/json")
            .header("Content-Type", "application/json")
            .header("Authorization", "Bearer " + token)
            .POST(HttpRequest.BodyPublishers.ofString(body))
            .build();

    int retry = 0;
    HttpResponse<String> response = null;
    Exception lastExc = null;
    while (response == null && retry < 10) {
      try {
        response = client.send(request, HttpResponse.BodyHandlers.ofString());
      } catch (Exception e) {
        lastExc = e;
        response = null;
        retry++;

        try {
          TimeUnit.SECONDS.sleep(2 * retry);
        } catch (Exception ee) {
        }
      }
    }

    if (response == null) {
      throw new RuntimeException("No response in " + retry + " retries", lastExc);
    }

    String resp_body = response.body();

    try {
      Map<String, List> map = mapper.readValue(resp_body, HashMap.class);
      List element = (List) map.get("data").get(0);
      List array = (List) element.get(1);
      return (List<Double>) array;
    } catch (JsonProcessingException e) {
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
