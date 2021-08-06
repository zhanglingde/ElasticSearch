package highlevel;

import org.apache.http.HttpHost;
import org.elasticsearch.action.admin.indices.alias.Alias;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;

import org.apache.http.HttpHost;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentType;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * @author zhangling  2021/8/4 16:58
 */
public class HighLevelTest {
    public static void main(String[] args) throws IOException {
        // RestHighLevelClient client = new RestHighLevelClient(RestClient.builder(
        //         new HttpHost("192.168.152.132", 9200, "http")
        // ));
        // // 删除已经存在的索引
        // DeleteIndexRequest deleteIndexRequest = new DeleteIndexRequest("blog");
        // client.indices().delete(deleteIndexRequest, RequestOptions.DEFAULT);
        // // 创建一个索引
        // CreateIndexRequest blog = new CreateIndexRequest("blog");
        // // 配置 settings，分片、副本等信息
        // blog.settings(Settings.builder().put("index.number_of_shards",3).put("index.number_of_replicas",2));
        // // 配置字段类型，字段类型可以通过 JSON 字符串、Map 以及 XContentBuilder 三种方式创建
        // // Json 字符串方式
        // blog.mapping("{\"properties\":{\"title\":{\"type\":\"text\"}}}", XContentType.JSON);
        // // 执行请求，创建索引
        // client.indices().create(blog, RequestOptions.DEFAULT);
        // client.close();

        // mapMapping();
        xContentBuilderMapping();
    }

    // map 配置 Mapping
    public static void mapMapping() throws IOException{
        RestHighLevelClient client = new RestHighLevelClient(RestClient.builder(
                new HttpHost("192.168.152.132", 9200, "http")
        ));
        // 删除已经存在的索引
        DeleteIndexRequest deleteIndexRequest = new DeleteIndexRequest("blog");
        client.indices().delete(deleteIndexRequest, RequestOptions.DEFAULT);
        // 创建一个索引
        CreateIndexRequest blog = new CreateIndexRequest("blog");
        // 配置 settings，分片、副本等信息
        blog.settings(Settings.builder().put("index.number_of_shards",3).put("index.number_of_replicas",2));
        // 配置字段类型，字段类型可以通过 JSON 字符串、Map 以及 XContentBuilder 三种方式创建
        // Json 字符串方式
        // blog.mapping("{\"properties\":{\"title\":{\"type\":\"text\"}}}", XContentType.JSON);
        // map 方式
        Map<String, String> title = new HashMap<>();
        title.put("type", "text");
        Map<String, Object> properties = new HashMap<>();
        properties.put("title", title);
        Map<String,Object> mappings = new HashMap<>();
        mappings.put("properties", properties);
        blog.mapping(mappings);
        // 执行请求，创建索引
        client.indices().create(blog, RequestOptions.DEFAULT);
        client.close();
    }

    // XContentBuilder 方式设置 mapping
    public static void xContentBuilderMapping() throws IOException{
        RestHighLevelClient client = new RestHighLevelClient(RestClient.builder(
                new HttpHost("192.168.152.132", 9200, "http")
        ));
        // 删除已经存在的索引
        DeleteIndexRequest deleteIndexRequest = new DeleteIndexRequest("blog");
        client.indices().delete(deleteIndexRequest, RequestOptions.DEFAULT);
        // 创建一个索引
        CreateIndexRequest blog = new CreateIndexRequest("blog");
        // 配置 settings，分片、副本等信息
        blog.settings(Settings.builder().put("index.number_of_shards",3).put("index.number_of_replicas",2));
        // 配置字段类型，字段类型可以通过 JSON 字符串、Map 以及 XContentBuilder 三种方式创建
        // Json 字符串方式
        // blog.mapping("{\"properties\":{\"title\":{\"type\":\"text\"}}}", XContentType.JSON);
        // map 方式
        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startObject();
        builder.startObject("properties");
        builder.startObject("title");
        builder.field("type", "text");
        builder.endObject();
        builder.endObject();
        builder.endObject();
        blog.mapping(builder);

        // 配置别名
        blog.alias(new Alias("blog_alias"));
        // 执行请求，创建索引
        client.indices().create(blog, RequestOptions.DEFAULT);
        client.close();
    }


}
