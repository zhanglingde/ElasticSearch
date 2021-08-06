package highlevel;

import com.sun.javafx.property.adapter.ReadOnlyPropertyDescriptor;
import org.apache.http.HttpHost;
import org.elasticsearch.action.admin.indices.flush.FlushRequest;
import org.elasticsearch.action.admin.indices.open.OpenIndexRequest;
import org.elasticsearch.action.admin.indices.refresh.RefreshRequest;
import org.elasticsearch.action.admin.indices.settings.get.GetSettingsRequest;
import org.elasticsearch.action.admin.indices.settings.get.GetSettingsResponse;
import org.elasticsearch.action.admin.indices.settings.put.UpdateSettingsRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CloseIndexRequest;
import org.elasticsearch.client.indices.CloseIndexResponse;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.client.indices.ResizeRequest;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.settings.Settings;

import java.io.IOException;
import java.util.List;

/**
 *
 * 索引操作
 */
public class IndexOperation {
    public static void main(String[] args) throws IOException{
        // isExist();
        // closeIndex();
        // openIndex();
        // updateIndex();
        // cloneIndex();
        showIndex();
    }




    // 索引是否存在
    private static void isExist() throws IOException {
        RestHighLevelClient client = new RestHighLevelClient(RestClient.builder(
                new HttpHost("192.168.152.132", 9200, "http")
        ));
        GetIndexRequest blog = new GetIndexRequest("blog");
        boolean exists = client.indices().exists(blog, RequestOptions.DEFAULT);
        System.out.println("exists = " + exists);
        client.close();
    }

    // 关闭索引
    public static void closeIndex() throws IOException{
        RestHighLevelClient client = new RestHighLevelClient(RestClient.builder(
                new HttpHost("192.168.152.132", 9200, "http")
        ));
        CloseIndexRequest blog = new CloseIndexRequest("blog");
        CloseIndexResponse close = client.indices().close(blog, RequestOptions.DEFAULT);
        List<CloseIndexResponse.IndexResult> indices = close.getIndices();
        for (CloseIndexResponse.IndexResult index : indices) {
            System.out.println("index.getIndex() = " + index.getIndex());
        }
        client.close();
    }

    // 打开索引
    public static void openIndex() throws IOException{
        RestHighLevelClient client = new RestHighLevelClient(RestClient.builder(
                new HttpHost("192.168.152.132", 9200, "http")
        ));
        OpenIndexRequest blog = new OpenIndexRequest("blog");
        client.indices().open(blog, RequestOptions.DEFAULT);

        client.close();
    }

    // 修改索引
    public static void updateIndex() throws IOException{
        RestHighLevelClient client = new RestHighLevelClient(RestClient.builder(
                new HttpHost("192.168.152.132", 9200, "http")
        ));
        UpdateSettingsRequest request = new UpdateSettingsRequest("blog");
        request.settings(Settings.builder().put("index.blocks.write",true).build());
        client.indices().putSettings(request, RequestOptions.DEFAULT);

        client.close();
    }

    // 克隆索引（索引要只读才可以克隆）
    public static void cloneIndex() throws IOException{
        RestHighLevelClient client = new RestHighLevelClient(RestClient.builder(
                new HttpHost("192.168.152.132", 9200, "http")
        ));
        ResizeRequest request = new ResizeRequest("blog2","blog");

        client.indices().clone(request, RequestOptions.DEFAULT);
        client.close();
    }

    // 查看索引
    public static void showIndex() throws IOException{
        RestHighLevelClient client = new RestHighLevelClient(RestClient.builder(
                new HttpHost("192.168.152.132", 9200, "http")
        ));
        GetSettingsRequest request = new GetSettingsRequest().indices("books");
        // 设置需要的具体参数，不设置则返回所有参数
        request.names("index.blocks.write");

        GetSettingsResponse response = client.indices().getSettings(request, RequestOptions.DEFAULT);
        ImmutableOpenMap<String, Settings> indexToSettings = response.getIndexToSettings();
        System.out.println(indexToSettings);
        String s = response.getSetting("product", "index.number_of_replicas");
        System.out.println(s);
        client.close();
    }

    // refresh
    public static void refreshIndex() throws IOException{
        RestHighLevelClient client = new RestHighLevelClient(RestClient.builder(
                new HttpHost("192.168.152.132", 9200, "http")
        ));
        RefreshRequest request = new RefreshRequest("books");
        client.indices().refresh(request, RequestOptions.DEFAULT);
        FlushRequest flushRequest = new FlushRequest("blog");
        client.indices().flush(flushRequest, RequestOptions.DEFAULT);

        client.close();
    }






}
