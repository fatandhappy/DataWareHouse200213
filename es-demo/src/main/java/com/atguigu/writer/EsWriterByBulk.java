package com.atguigu.writer;

import com.atguigu.bean.Movie;
import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.config.HttpClientConfig;
import io.searchbox.core.Bulk;
import io.searchbox.core.Index;

import java.io.IOException;

/**
 * @author zhangjie
 * @create 2020-07-11 11:28
 * @description
 */
public class EsWriterByBulk {
    public static void main(String[] args) throws IOException {
        //1.创建工厂
        JestClientFactory jcFactory = new JestClientFactory();

        //2.创建配置信息
        HttpClientConfig hcconfig = new HttpClientConfig.Builder("http://hadoop102:9200").build();

        //3.设置配置信息
        jcFactory.setHttpClientConfig(hcconfig);
        
        //4.获取客户端对象
        JestClient jc = jcFactory.getObject();

        //5.创建多个Index对象
        Movie movie1 = new Movie("0005", "少年派的奇幻漂流");
        Movie movie2 = new Movie("0006", "星际穿越");
        Movie movie3 = new Movie("0007", "黑鹰坠落");

        Index index1 = new Index.Builder(movie1).id("1005").build();
        Index index2 = new Index.Builder(movie2).id("1006").build();
        Index index3 = new Index.Builder(movie3).id("1007").build();

        //6.创建Bulk对象
        Bulk.Builder builder = new Bulk.Builder()
                .addAction(index1)
                .addAction(index2)
                .addAction(index3)
                .defaultIndex("movie")
                .defaultType("_doc");

        Bulk bulk = builder.build();

        //7.执行批量插入数据操作
        jc.execute(bulk);

        //8.关闭资源
        jc.close();
    }
}
