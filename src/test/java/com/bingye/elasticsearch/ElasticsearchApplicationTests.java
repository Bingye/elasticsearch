package com.bingye.elasticsearch;

import com.alibaba.fastjson.JSON;
import com.bingye.elasticsearch.domain.pojo.User;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.CreateIndexResponse;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.*;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightField;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

@SpringBootTest
class ElasticsearchApplicationTests {

    @Autowired
    public RestHighLevelClient restHighLevelClient;

    //创建索引
    @Test
    void createIndex() throws IOException {
        //创建索引请求对象
        CreateIndexRequest requet = new CreateIndexRequest("index");
        //执行请求
        CreateIndexResponse createIndexResponse = restHighLevelClient.indices().create(requet, RequestOptions.DEFAULT);
        //打印返回结果
        System.out.println(JSON.toJSONString(createIndexResponse));
    }

    //获取索引
    @Test
    void getIndex() throws IOException {
        GetIndexRequest request = new GetIndexRequest("index");
        boolean exists = restHighLevelClient.indices().exists(request, RequestOptions.DEFAULT);
        System.out.println(exists);
    }

    //删除索引
    @Test
    void deleteIndex() throws IOException {
        DeleteIndexRequest request = new DeleteIndexRequest("index");
        AcknowledgedResponse delete = restHighLevelClient.indices().delete(request, RequestOptions.DEFAULT);
        System.out.println(delete.isAcknowledged());
    }

    //创建文档
    @Test
    void createDocument() throws IOException {
        User user = new User("1","叶兵", 1, "小学生");

        IndexRequest request = new IndexRequest("index");
        request.id("2");
        request.timeout(TimeValue.timeValueSeconds(1));
        request.source(JSON.toJSONString(user), XContentType.JSON);

        IndexResponse indexResponse = restHighLevelClient.index(request, RequestOptions.DEFAULT);
        System.out.println(JSON.toJSONString(indexResponse));
    }

    //获取文档
    @Test
    void getDocument() throws IOException {
        GetRequest request = new GetRequest("index","1");
        //不获取返回的_source 的上下文
        //request.fetchSourceContext(new FetchSourceContext(false));
        boolean exists = restHighLevelClient.exists(request, RequestOptions.DEFAULT);
        if(exists){
            GetResponse response = restHighLevelClient.get(request, RequestOptions.DEFAULT);
            System.out.println(JSON.toJSONString(response));
        }
        System.out.println(exists);
    }

    //更新文档
    @Test
    void updateDocument() throws IOException {
        User user = new User("1","叶并", 1, "哈哈哈");
        UpdateRequest request = new UpdateRequest("index", "1");
        request.doc(JSON.toJSONString(user),XContentType.JSON);
        UpdateResponse response = restHighLevelClient.update(request, RequestOptions.DEFAULT);
        System.out.println(JSON.toJSONString(response));
    }

    @Test
    void deleteDocument() throws IOException {
        DeleteRequest request = new DeleteRequest("index", "2");
        DeleteResponse delete = restHighLevelClient.delete(request, RequestOptions.DEFAULT);
        System.out.println(delete);
    }

    @Test
    void bulkRequest() throws IOException {
        BulkRequest bulkRequest = new BulkRequest();
        bulkRequest.timeout("10s");

        ArrayList<User> users = new ArrayList<>();
        users.add(new User("2","张三",1,"张三xxx"));
        users.add(new User("3","李四",2,"李四xxx"));
        users.add(new User("4","王五",3,"王五xxx"));

        users.forEach(user -> {
            IndexRequest indexRequest = new IndexRequest("index");
            indexRequest.id(user.getId());
            indexRequest.source(JSON.toJSONString(user),XContentType.JSON);
            //批量添加
            bulkRequest.add(indexRequest);
            //批量更新
            //bulkRequest.add(new UpdateRequest("index"))
            //批量删除
            //bulkRequest.add(new DeleteRequest("index"));
        });

        BulkResponse bulkResponse = restHighLevelClient.bulk(bulkRequest, RequestOptions.DEFAULT);
        System.out.println(JSON.toJSONString(bulkResponse));
    }

    @Test
    void seachDocument() throws IOException {
        SearchRequest searchRequest = new SearchRequest("index");
        //构建搜索的条件
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();

        //查询构建
        //精确查询
        //TermQueryBuilder termQueryBuilder = QueryBuilders.termQuery("name", "并");
        //searchSourceBuilder.query(termQueryBuilder);

        //MatchPhraseQueryBuilder matchPhraseQueryBuilder = QueryBuilders.matchPhraseQuery("name", "叶并");
        //searchSourceBuilder.query(matchPhraseQueryBuilder);

        BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
        List<QueryBuilder> should = boolQueryBuilder.should();
        should.add(QueryBuilders.matchPhraseQuery("name", "叶"));
        should.add(QueryBuilders.matchPhraseQuery("desc", "张三"));
        searchSourceBuilder.query(boolQueryBuilder);

        //匹配所有
        //MatchAllQueryBuilder matchAllQueryBuilder = QueryBuilders.matchAllQuery();
        //searchSourceBuilder.query(matchAllQueryBuilder);

        //高亮的构建
        HighlightBuilder highlightBuilder = new HighlightBuilder();
        highlightBuilder.preTags("<span style='color:red'>");
        highlightBuilder.postTags("</span>");
        highlightBuilder.field("name").field("desc");
        highlightBuilder.requireFieldMatch(false); //允许多个高亮
        searchSourceBuilder.highlighter(highlightBuilder);

        //分页
        searchSourceBuilder.from(0);
        searchSourceBuilder.size(10);

        //设置超时时间
        searchSourceBuilder.timeout(new TimeValue(60, TimeUnit.SECONDS));

        searchRequest.source(searchSourceBuilder);

        SearchResponse searchReponse = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);
        System.out.println(JSON.toJSONString(searchReponse.getHits()));
        for (SearchHit hit : searchReponse.getHits().getHits()) {

            Map<String, HighlightField> highlightFields = hit.getHighlightFields();
            HighlightField name = highlightFields.get("name");
            HighlightField desc = highlightFields.get("desc");
            Map<String, Object> sourceAsMap = hit.getSourceAsMap();

            if(name!=null){
                Text[] fragments = name.fragments();
                String n_name="";
                for (Text fragment : fragments) {
                    n_name += fragment;
                }
                sourceAsMap.put("name",n_name);

                Text[] fragments2 = name.fragments();
                String n_desc="";
                for (Text fragment2 : fragments2) {
                    n_desc += fragment2;
                }
                sourceAsMap.put("desc",n_desc);
            }
            System.out.println(sourceAsMap);

        }
    }
}
