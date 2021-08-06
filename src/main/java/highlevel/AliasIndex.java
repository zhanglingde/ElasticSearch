package highlevel;

import org.apache.http.HttpHost;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest;
import org.elasticsearch.action.admin.indices.alias.get.GetAliasesRequest;
import org.elasticsearch.client.GetAliasesResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.DeleteAliasRequest;
import org.elasticsearch.cluster.metadata.AliasMetadata;

import java.io.IOException;
import java.util.Map;
import java.util.Set;

/**
 * 索引别名
 */
public class AliasIndex {
    public static void main(String[] args) throws IOException{
        addAlias();
        // addFilterAlias();
        // deleteAlias();
        // deleteAlias2();
        isExist();
        getAlias();
    }

    // 添加普通别名
    public static void addAlias() throws IOException {
        RestHighLevelClient client = new RestHighLevelClient(RestClient.builder(
                new HttpHost("192.168.152.132", 9200, "http")
        ));
        IndicesAliasesRequest indicesAliasesRequest = new IndicesAliasesRequest();
        IndicesAliasesRequest.AliasActions aliasActions = new IndicesAliasesRequest.AliasActions(IndicesAliasesRequest.AliasActions.Type.ADD);
        aliasActions.index("books").alias("books_alias");
        indicesAliasesRequest.addAliasAction(aliasActions);
        client.indices().updateAliases(indicesAliasesRequest, RequestOptions.DEFAULT);
        client.close();
    }

    // 添加 filter 别名
    public static void addFilterAlias() throws IOException {
        RestHighLevelClient client = new RestHighLevelClient(RestClient.builder(
                new HttpHost("192.168.152.132", 9200, "http")
        ));
        IndicesAliasesRequest indicesAliasesRequest = new IndicesAliasesRequest();
        IndicesAliasesRequest.AliasActions aliasActions = new IndicesAliasesRequest.AliasActions(IndicesAliasesRequest.AliasActions.Type.ADD);
        aliasActions.index("books").alias("books_alias2").filter("\"term\":{\"name\":\"java\"}");
        indicesAliasesRequest.addAliasAction(aliasActions);
        client.indices().updateAliases(indicesAliasesRequest, RequestOptions.DEFAULT);
        client.close();
    }

    // 删除别名
    public static void deleteAlias() throws IOException {
        RestHighLevelClient client = new RestHighLevelClient(RestClient.builder(
                new HttpHost("192.168.152.132", 9200, "http")
        ));
        IndicesAliasesRequest indicesAliasesRequest = new IndicesAliasesRequest();
        IndicesAliasesRequest.AliasActions aliasActions = new IndicesAliasesRequest.AliasActions(IndicesAliasesRequest.AliasActions.Type.REMOVE);
        aliasActions.index("books").alias("books_alias");
        indicesAliasesRequest.addAliasAction(aliasActions);
        client.indices().updateAliases(indicesAliasesRequest, RequestOptions.DEFAULT);
        client.close();
    }

    // 删除别名
    public static void deleteAlias2() throws IOException {
        RestHighLevelClient client = new RestHighLevelClient(RestClient.builder(
                new HttpHost("192.168.152.132", 9200, "http")
        ));
        DeleteAliasRequest deleteAliasRequest = new DeleteAliasRequest("books", "books_alias");
        client.indices().deleteAlias(deleteAliasRequest, RequestOptions.DEFAULT);
        client.close();
    }

    // 判断别名是否存在
    public static void isExist() throws IOException {
        RestHighLevelClient client = new RestHighLevelClient(RestClient.builder(
                new HttpHost("192.168.152.132", 9200, "http")
        ));
        GetAliasesRequest books_alias = new GetAliasesRequest("books_alias");
        // 指定查看某一个索引的别名，不指定，则会搜索所有的别名
        books_alias.indices("books");
        boolean b = client.indices().existsAlias(books_alias, RequestOptions.DEFAULT);
        System.out.println("b = " + b);
        client.close();
    }

    // 获取别名
    public static void getAlias() throws IOException {
        RestHighLevelClient client = new RestHighLevelClient(RestClient.builder(
                new HttpHost("192.168.152.132", 9200, "http")
        ));
        GetAliasesRequest books_alias = new GetAliasesRequest("books_alias");
        // 指定查看某一个索引的别名，不指定，则会搜索所有的别名
        books_alias.indices("books");
        GetAliasesResponse response = client.indices().getAlias(books_alias, RequestOptions.DEFAULT);
        Map<String, Set<AliasMetadata>> aliases = response.getAliases();
        System.out.println("aliases = " + aliases);

        client.close();
    }







}
