package com.atguigu.reader;

import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.config.HttpClientConfig;
import io.searchbox.core.Search;
import io.searchbox.core.SearchResult;
import io.searchbox.core.search.aggregation.MetricAggregation;
import io.searchbox.core.search.aggregation.TermsAggregation;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * @author zhangjie
 * @create 2020-07-11 11:27
 * @description
 */
public class EsReader {
    public static void main(String[] args) throws IOException {
        //1.创建工厂
        JestClientFactory jcFactory = new JestClientFactory();

        //2.创建配置信息
        HttpClientConfig hcconfig = new HttpClientConfig.Builder("http://hadoop102:9200").build();

        //3.设置配置信息
        jcFactory.setHttpClientConfig(hcconfig);

        //4.获取客户端对象
        JestClient jc = jcFactory.getObject();

        //5.创建Search对象
        Search search = new Search.Builder("{\n" +
                "  \"query\": {\n" +
                "    \"bool\": {\n" +
                "      \"filter\": {\n" +
                "        \"term\": {\n" +
                "          \"sex\": \"male\"\n" +
                "        }\n" +
                "      },\n" +
                "      \"must\": [\n" +
                "        {\n" +
                "          \"match\": {\n" +
                "            \"favor\": \"游戏\"\n" +
                "          }\n" +
                "        }\n" +
                "      ]\n" +
                "    }\n" +
                "  },\n" +
                "  \"aggs\": {\n" +
                "    \"group_by_favor\": {\n" +
                "      \"terms\": {\n" +
                "        \"field\": \"age\",\n" +
                "        \"size\": 10\n" +
                "      }\n" +
                "    }\n" +
                "  },\n" +
                "  \"from\": 1,\n" +
                "  \"size\": 20\n" +
                "}")
                .addIndex("student")
                .addType("_doc")
                .build();

        //6.执行查询操作
        SearchResult searchResult = jc.execute(search);

        //7.解析searchResult
        //获取总数
        System.out.println("总计数据：" + searchResult.getTotal() + "条！");

        //获取最高分
        System.out.println("最高分为：" + searchResult.getMaxScore());

        //获取数据明细
        List<SearchResult.Hit<Map, Void>> hits = searchResult.getHits(Map.class);
        for (SearchResult.Hit<Map, Void> hit : hits) {
            Map source = hit.source;
            for (Object key : source.keySet()) {
                System.out.println("key: " + key + ", value: " + source.get(key));
            }
            System.out.println("===========================");
        }

        //获取聚合组
        MetricAggregation aggregations = searchResult.getAggregations();
        TermsAggregation group_by_favor = aggregations.getTermsAggregation("group_by_favor");
        List<TermsAggregation.Entry> buckets = group_by_favor.getBuckets();
        for (TermsAggregation.Entry bucket : buckets) {
            System.out.println("key: " + bucket.getKey() + ", count: " + bucket.getCount());
            System.out.println("++++++++++++++++++++++++++");
        }

        //关闭资源
        jc.close();
    }
}
