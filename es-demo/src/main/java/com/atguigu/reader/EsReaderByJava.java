package com.atguigu.reader;

import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.config.HttpClientConfig;
import io.searchbox.core.Search;
import io.searchbox.core.SearchResult;
import org.apache.lucene.queryparser.xml.builders.BooleanQueryBuilder;
import org.apache.lucene.search.BooleanQuery;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * @author zhangjie
 * @create 2020-07-11 19:02
 * @description
 */
public class EsReaderByJava {
    public static void main(String[] args) throws IOException {
        JestClientFactory jcFactory = new JestClientFactory();

        HttpClientConfig config = new HttpClientConfig.Builder("http://hadoop102:9200").build();

        jcFactory.setHttpClientConfig(config);

        JestClient jc = jcFactory.getObject();

        //5.创建DSL语句构建对象
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();

        BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();

        //定义全值匹配过滤器
        TermQueryBuilder termQueryBuilder = new TermQueryBuilder("sex", "male");
        boolQueryBuilder.filter(termQueryBuilder);

        //定义分词匹配过滤器
        MatchQueryBuilder matchQueryBuilder = new MatchQueryBuilder("favor", "游戏");
        boolQueryBuilder.filter(matchQueryBuilder);

        searchSourceBuilder.query(boolQueryBuilder);

        // 创建Search对象
        Search search = new Search.Builder(searchSourceBuilder.toString())
                .addIndex("student")
                .addType("_doc")
                .build();

        SearchResult searchResult = jc.execute(search);

        //8.解析searchResult
        System.out.println("总计： " + searchResult.getTotal() + " 条数据");
        System.out.println("最高分为： " + searchResult.getMaxScore());
        List<SearchResult.Hit<Map, Void>> hits = searchResult.getHits(Map.class);
        for (SearchResult.Hit<Map, Void> hit : hits) {
            Map source = hit.source;
            Set set = source.keySet();
            for (Object key : set) {
                System.out.println("key: " + key + ", value" + source.get(key));
            }
            System.out.println("========================");
        }

        jc.close();
    }
}
