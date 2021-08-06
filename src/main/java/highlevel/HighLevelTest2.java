package highlevel;

import org.apache.http.HttpHost;
import org.elasticsearch.action.admin.indices.alias.Alias;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * json 创建索引
 */
public class HighLevelTest2 {
    public static void main(String[] args) throws IOException {
        RestHighLevelClient client = new RestHighLevelClient(RestClient.builder(
                new HttpHost("192.168.152.132", 9200, "http")
        ));
        // 删除已经存在的索引
        DeleteIndexRequest deleteIndexRequest = new DeleteIndexRequest("blog");
        client.indices().delete(deleteIndexRequest, RequestOptions.DEFAULT);
        // 创建一个索引
        CreateIndexRequest blog = new CreateIndexRequest("blog");
        // Json 配置索引
        blog.source("{\n" +
                "  \"settings\": {\n" +
                "    \"number_of_shards\": 3,\n" +
                "    \"number_of_replicas\": 2\n" +
                "  },\n" +
                "  \"mappings\": {\n" +
                "    \"properties\": {\n" +
                "      \"title\":{\n" +
                "        \"type\": \"keyword\"\n" +
                "      }\n" +
                "    }\n" +
                "  },\n" +
                "  \"aliases\": {\n" +
                "    \"blog_alias\": {}\n" +
                "  }\n" +
                "}",XContentType.JSON);
        // 请求超时时间，连接所有节点的超时时间
        blog.setTimeout(TimeValue.timeValueMillis(2));
        // 连接 master 接点的超时时间
        blog.setMasterTimeout(TimeValue.timeValueMillis(1));
        // 执行请求，创建索引
        client.indices().create(blog, RequestOptions.DEFAULT);
        client.close();

    }



    
}
