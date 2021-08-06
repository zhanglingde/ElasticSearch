package highlevel;

import org.apache.http.HttpHost;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.support.replication.ReplicationResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * 文档操作
 */
public class DocumentOperation {
    public static void main(String[] args) throws IOException{
        // addDoc();
        // getDoc();
        // existDoc();
        // updateDoc();
        // updateDocJson();
        // updateDocMap();
        // updateDocXContentBuilder();
        upsert();
    }



    // 添加文档
    public static void addDoc() throws IOException {
        RestHighLevelClient client = new RestHighLevelClient(RestClient.builder(
                new HttpHost("192.168.152.132", 9200, "http")
        ));
        // 构建一个 IndexRequest 请求，参数为文档索引的索引名称
        IndexRequest request = new IndexRequest("book");
        // 给请求配置一个 id，即文档 id。如果指定了 id，相当于 put book/_doc/id。也可以不指定 id，相当于 post book/_doc
        // request.id("1");
        // 构建索引文档，有三种方式：Json 字符串、Map 对象、XContentBuilder
        // request.source("{\n" +
        //         "  \"name\":\"三国演义\",\n" +
        //         "  \"author\":\"罗贯中\"\n" +
        //         "}", XContentType.JSON);

        // Map 方式构建索引文档
        // Map<String, String> map = new HashMap<>();
        // map.put("name", "水浒传");
        // map.put("author", "施耐庵");
        // request.source(map).id("2");
        // XContentBuilder 方式
        XContentBuilder jsonBuilder = XContentFactory.jsonBuilder();
        jsonBuilder.startObject();
        jsonBuilder.field("name", "西游记");
        jsonBuilder.field("author", "吴承恩");
        jsonBuilder.endObject();
        request.source(jsonBuilder);
        // 同步执行请求
        IndexResponse indexResponse = client.index(request, RequestOptions.DEFAULT);
        // 获取文档 id
        String id = indexResponse.getId();
        System.out.println("id = " + id);
        // 获取索引名称
        String index = indexResponse.getIndex();
        System.out.println("index = " + index);
        // 判断文档是否添加成功
        if (indexResponse.getResult() == DocWriteResponse.Result.CREATED) {
            System.out.println("文档添加成功");
        }
        if (indexResponse.getResult() == DocWriteResponse.Result.UPDATED) {
            System.out.println("文档更新成功");
        }
        ReplicationResponse.ShardInfo shardInfo = indexResponse.getShardInfo();
        // 判断分片操作是否都成功
        if (shardInfo.getTotal() != shardInfo.getSuccessful()) {
            System.out.println("有存在问题的分片");
        }
        // 有存在问题的分片
        if (shardInfo.getFailed() > 0) {
            // 打印错误信息
            for (ReplicationResponse.ShardInfo.Failure failure : shardInfo.getFailures()) {
                System.out.println("failure.reason() = " + failure.reason());
            }
        }
        client.close();

    }

    // 根据 id 获取文档
    public static void getDoc() throws IOException{
        RestHighLevelClient client = new RestHighLevelClient(RestClient.builder(
                new HttpHost("192.168.152.132", 9200, "http")
        ));
        GetRequest request = new GetRequest("book", "2");
        GetResponse response = client.get(request, RequestOptions.DEFAULT);
        System.out.println("response.getId() = " + response.getId());
        System.out.println("response.getIndex() = " + response.getIndex());
        if (response.isExists()) {
            // 文档存在
            long version = response.getVersion();
            System.out.println("version = " + version);
            String sourceAsString = response.getSourceAsString();
            System.out.println("sourceAsString = " + sourceAsString);
        }else {
            System.out.println("文档不存在");
        }
        client.close();
    }

    // 判断文档是否存在
    public static void existDoc() throws IOException{
        RestHighLevelClient client = new RestHighLevelClient(RestClient.builder(
                new HttpHost("192.168.152.132", 9200, "http")
        ));
        GetRequest request = new GetRequest("book", "2");
        request.fetchSourceContext(new FetchSourceContext(false));
        boolean exists = client.exists(request, RequestOptions.DEFAULT);
        System.out.println("exists = " + exists);
        client.close();
    }

    // 根据 id 删除文档
    public static void deleteDoc() throws IOException{
        RestHighLevelClient client = new RestHighLevelClient(RestClient.builder(
                new HttpHost("192.168.152.132", 9200, "http")
        ));
        DeleteRequest request = new DeleteRequest("book", "3");
        DeleteResponse response = client.delete(request, RequestOptions.DEFAULT);
        System.out.println("response.getId() = " + response.getId());
        System.out.println("response.getIndex() = " + response.getIndex());
        System.out.println("response.getVersion() = " + response.getVersion());
        ReplicationResponse.ShardInfo shardInfo = response.getShardInfo();
        if (shardInfo.getTotal() != shardInfo.getSuccessful()) {
            System.out.println("有分片存在问题");
        }
        if (shardInfo.getFailed() > 0) {
            for (ReplicationResponse.ShardInfo.Failure failure : shardInfo.getFailures()) {
                System.out.println("failure.reason() = " + failure.reason());
            }
        }
        client.close();
    }

    // 通过脚本更新
    public static void updateDoc() throws IOException{
        RestHighLevelClient client = new RestHighLevelClient(RestClient.builder(
                new HttpHost("192.168.152.132", 9200, "http")
        ));
        UpdateRequest request = new UpdateRequest("book", "2");
        // 通过脚本更新
        Map<String, Object> params = Collections.singletonMap("name", "三国演义6");
        Script inline = new Script(ScriptType.INLINE, "painless", "ctx._source.name=params.name", params);
        request.script(inline);
        UpdateResponse response = client.update(request, RequestOptions.DEFAULT);

        System.out.println("response.getId() = " + response.getId());
        System.out.println("response.getIndex() = " + response.getIndex());
        System.out.println("response.getVersion() = " + response.getVersion());
        if (response.getResult() == DocWriteResponse.Result.UPDATED) {
            System.out.println("更新成功！");
        }
        client.close();
    }

    // 通过 json 更新文档
    public static void updateDocJson() throws IOException{
        RestHighLevelClient client = new RestHighLevelClient(RestClient.builder(
                new HttpHost("192.168.152.132", 9200, "http")
        ));
        UpdateRequest request = new UpdateRequest("book", "2");
        // 通过 json 更新
        request.doc("{\n" +
                "  \"name\":\"三国演义\"\n" +
                "}", XContentType.JSON);
        UpdateResponse response = client.update(request, RequestOptions.DEFAULT);

        System.out.println("response.getId() = " + response.getId());
        System.out.println("response.getIndex() = " + response.getIndex());
        System.out.println("response.getVersion() = " + response.getVersion());
        if (response.getResult() == DocWriteResponse.Result.UPDATED) {
            System.out.println("更新成功！");
        }
        client.close();
    }

    // map 构建 json参数
    public static void updateDocMap() throws IOException{
        RestHighLevelClient client = new RestHighLevelClient(RestClient.builder(
                new HttpHost("192.168.152.132", 9200, "http")
        ));
        UpdateRequest request = new UpdateRequest("book", "2");
        // map 构建 json 参数
        Map<String,Object> docMap = new HashMap<>();
        docMap.put("name","三国演义88");
        request.doc(docMap);
        UpdateResponse response = client.update(request, RequestOptions.DEFAULT);

        System.out.println("response.getId() = " + response.getId());
        System.out.println("response.getIndex() = " + response.getIndex());
        System.out.println("response.getVersion() = " + response.getVersion());
        if (response.getResult() == DocWriteResponse.Result.UPDATED) {
            System.out.println("更新成功！");
        }
        client.close();
    }
    // XContentBuilder 构建 json参数
    public static void updateDocXContentBuilder() throws IOException{
        RestHighLevelClient client = new RestHighLevelClient(RestClient.builder(
                new HttpHost("192.168.152.132", 9200, "http")
        ));
        UpdateRequest request = new UpdateRequest("book", "2");
        // XContentBuilder 构建 json 参数
        XContentBuilder jsonBuilder = XContentFactory.jsonBuilder();
        jsonBuilder.startObject();
        jsonBuilder.field("name", "三国演义666");
        jsonBuilder.endObject();
        request.doc(jsonBuilder);
        UpdateResponse response = client.update(request, RequestOptions.DEFAULT);

        System.out.println("response.getId() = " + response.getId());
        System.out.println("response.getIndex() = " + response.getIndex());
        System.out.println("response.getVersion() = " + response.getVersion());
        if (response.getResult() == DocWriteResponse.Result.UPDATED) {
            System.out.println("更新成功！");
        }
        client.close();
    }

    // upsert ,当文档不存在时，添加文档
    public static void upsert() throws IOException{
        RestHighLevelClient client = new RestHighLevelClient(RestClient.builder(
                new HttpHost("192.168.152.132", 9200, "http")
        ));
        UpdateRequest request = new UpdateRequest("book", "99");
        XContentBuilder jsonBuilder = XContentFactory.jsonBuilder();
        jsonBuilder.startObject();
        jsonBuilder.field("name", "三国演义123");
        jsonBuilder.endObject();
        request.doc(jsonBuilder);
        // id 99 文档不存在，就添加新文档
        request.upsert("{\n" +
                "  \"name\":\"红楼梦\",\n" +
                "  \"author\":\"曹雪芹\"\n" +
                "}", XContentType.JSON);
        UpdateResponse response = client.update(request, RequestOptions.DEFAULT);

        System.out.println("response.getId() = " + response.getId());
        System.out.println("response.getIndex() = " + response.getIndex());
        System.out.println("response.getVersion() = " + response.getVersion());
        if (response.getResult() == DocWriteResponse.Result.UPDATED) {
            System.out.println("更新成功！");
        } else if (response.getResult() == DocWriteResponse.Result.CREATED) {
            System.out.println("文档添加成功!");
        }
        client.close();

    }
}
