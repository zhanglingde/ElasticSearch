package lowlevel;

import org.apache.http.Header;
import org.apache.http.HttpHost;
import org.apache.http.entity.ContentType;
import org.apache.http.message.BasicHeader;
import org.apache.http.nio.entity.NStringEntity;
import org.elasticsearch.client.*;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

/**
 * @author zhangling  2021/8/4 14:16
 */
public class LowLevelTest {
    public static void main(String[] args) throws Exception {
        // // 1. 构建 RestClient 对象，集群可以有多个 HttpHost
        // RestClientBuilder builder = RestClient.builder(
        //         new HttpHost("192.168.152.132", 9200, "http")
        // );
        // // 2. 如果需要在请求头中设置认证信息，可以通过 builder 设置
        // // builder.setDefaultHeaders(new Header[]{new BasicHeader("key","value")});
        // RestClient restClient = builder.build();
        // // 3. 构建请求
        // Request request = new Request("GET", "/books/_search");
        // request.addParameter("pretty", "true");
        // // 4. 同步发起请求
        // Response response = restClient.performRequest(request);
        // // 5. 解析 response
        // BufferedReader buffer = new BufferedReader(new InputStreamReader(response.getEntity().getContent()));
        // String str = null;
        // while ((str = buffer.readLine()) != null) {
        //     System.out.println(str);
        // }
        // buffer.close();
        // restClient.close();

        jsonGet();
    }

    public static void asyncGet() throws Exception {
        // 1. 构建 RestClient 对象，集群可以有多个 HttpHost
        RestClientBuilder builder = RestClient.builder(
                new HttpHost("192.168.152.132", 9200, "http")
        );
        // 2. 如果需要在请求头中设置认证信息，可以通过 builder 设置
        // builder.setDefaultHeaders(new Header[]{new BasicHeader("key","value")});
        RestClient restClient = builder.build();
        // 3. 构建请求
        Request request = new Request("GET", "/books/_search");
        request.addParameter("pretty", "true");
        // 4. 异步发起请求
        restClient.performRequestAsync(request, new ResponseListener() {
            // 5. 异步请求成功解析 response
            @Override
            public void onSuccess(Response response) {
                try {
                    BufferedReader buffer = new BufferedReader(new InputStreamReader(response.getEntity().getContent()));
                    String str = null;
                    while ((str = buffer.readLine()) != null) {
                        System.out.println(str);
                    }
                    buffer.close();
                    restClient.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }

            @Override
            public void onFailure(Exception e) {

            }
        });
    }

    // 带 请求体json参数 的查询
    public static void jsonGet(){
        // 1. 构建 RestClient 对象，集群可以有多个 HttpHost
        RestClientBuilder builder = RestClient.builder(
                new HttpHost("192.168.152.132", 9200, "http")
        );
        // 2. 如果需要在请求头中设置认证信息，可以通过 builder 设置
        // builder.setDefaultHeaders(new Header[]{new BasicHeader("key","value")});
        RestClient restClient = builder.build();
        // 3. 构建请求
        Request request = new Request("GET", "/books/_search");
        request.addParameter("pretty", "true");
        // 添加请求体
        request.setEntity(new NStringEntity("{\"query\": {\"term\": {\"name\": {\"value\": \"java\"}}}}", ContentType.APPLICATION_JSON));
        // 4. 异步发起请求
        restClient.performRequestAsync(request, new ResponseListener() {
            // 5. 异步请求成功解析 response
            @Override
            public void onSuccess(Response response) {
                try {
                    BufferedReader buffer = new BufferedReader(new InputStreamReader(response.getEntity().getContent()));
                    String str = null;
                    while ((str = buffer.readLine()) != null) {
                        System.out.println(str);
                    }
                    buffer.close();
                    restClient.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }

            @Override
            public void onFailure(Exception e) {

            }
        });
    }
}
