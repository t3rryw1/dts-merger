package com.cozystay.notify;

import com.cozystay.model.SyncOperation;

import java.io.*;
import java.util.*;
import org.apache.http.HttpResponse;
import org.apache.http.NameValuePair;
import org.apache.http.client.HttpClient;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.message.BasicNameValuePair;
import net.sf.json.JSONObject;

public interface NotifyRule {

    enum requestMethod {
        POST,
        DELETE,
        PUT,
        GET
    }

    boolean operationMatchedRule(SyncOperation operation);

    NotifyAction acceptOperation(SyncOperation operation);

    class NotifyAction {
        public final String requestUrl;
        public final requestMethod requestMethod;
        public final Map<String, String> requestParams;
        public final Map<String, String> requestBody;

        private final String USER_AGENT = "5.0 (Macintosh; Intel Mac OS X 10_13_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/64.0.3282.167 Safari/537.36";

        NotifyAction() {
            requestUrl = null;
            requestMethod = null;
            requestParams = null;
            requestBody = null;
        }

        public NotifyAction (String url,
                             requestMethod method,
                             Map<String, String> params,
                             Map<String, String> body
        ) {
            this.requestUrl = url;
            this.requestMethod = method;
            this.requestParams = params;
            this.requestBody = body;
        }

        private JSONObject sendGet(String url) throws IOException {
            HttpClient client = new DefaultHttpClient();
            HttpGet request = new HttpGet(url);
            request.addHeader("User-Agent", USER_AGENT);
            HttpResponse response = client.execute(request);
            BufferedReader rd = new BufferedReader( new InputStreamReader(response.getEntity().getContent()) );

            StringBuilder result = new StringBuilder();
            String line;
            while ((line = rd.readLine()) != null) {
                result.append(line);
            }

            JSONObject resJson = JSONObject.fromObject(result.toString());
            return resJson;
        }

        private JSONObject sendPost(String url, Map<String, String> body) throws IOException {
            HttpClient client = new DefaultHttpClient();
            HttpPost post = new HttpPost(url);
            post.setHeader("User-Agent", USER_AGENT);

            List<NameValuePair> urlParameters = new ArrayList<>();
            for (Map.Entry<String, String> item : body.entrySet()) {
                urlParameters.add(new BasicNameValuePair(item.getKey(), item.getValue()));
            }

            post.setEntity(new UrlEncodedFormEntity(urlParameters));
            HttpResponse response = client.execute(post);
            BufferedReader rd = new BufferedReader( new InputStreamReader(response.getEntity().getContent()) );

            StringBuilder result = new StringBuilder();
            String line;
            while ((line = rd.readLine()) != null) {
                result.append(line);
            }

            JSONObject resJson = JSONObject.fromObject(result.toString());
            return resJson;
        }

        private JSONObject sendDelete(String url) throws IOException  {
            HttpClient client = new DefaultHttpClient();
            HttpDelete request = new HttpDelete(url);
            request.addHeader("User-Agent", USER_AGENT);
            HttpResponse response = client.execute(request);
            BufferedReader rd = new BufferedReader( new InputStreamReader(response.getEntity().getContent()) );

            StringBuilder result = new StringBuilder();
            String line;
            while ((line = rd.readLine()) != null) {
                result.append(line);
            }

            JSONObject resJson = JSONObject.fromObject(result.toString());
            return resJson;
        }

        public JSONObject sendRequest() throws IOException {
            switch (requestMethod) {
                case GET:
                    List<String> params = new ArrayList<>();
                    for (Map.Entry<String, String> param : requestParams.entrySet()) {
                        params.add(param.getKey() + "=" + param.getValue());
                    }
                    if (params.size() > 0) {
                        return sendGet(requestUrl + "?" + params.stream().reduce((o1, o2) -> o1+"&"+o2).get());
                    } else {
                        return sendGet(requestUrl);
                    }
                case POST:
                case PUT:
                    return sendPost(requestUrl, requestBody);
                case DELETE:
                    return sendDelete(requestUrl);
            }

            return null;
        }
    }
}
