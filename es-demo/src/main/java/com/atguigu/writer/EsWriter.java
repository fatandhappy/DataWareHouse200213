package com.atguigu.writer;

import com.atguigu.bean.Movie;
import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.config.HttpClientConfig;
import io.searchbox.core.Index;

import java.io.IOException;

/**
 * @author zhangjie
 * @create 2020-07-11 9:19
 * @description
 */
public class EsWriter {
    public static void main(String[] args) throws IOException {
        //1.创建ES客户端连接池
        JestClientFactory jcFactory = new JestClientFactory();

        //2.创建ES客户端连接地址
        HttpClientConfig hcConfig = new HttpClientConfig.Builder("http://hadoop102:9200").build();

        //3.设置ES连接地址
        jcFactory.setHttpClientConfig(hcConfig);

        //4.获取ES客户端连接
        JestClient jc = jcFactory.getObject();

        //5.构建ES插入数据对象
//        Index index = new Index.Builder("{\n" +
//                "  \"movie_id\":\"0003\",\n" +
//                "  \"movie_name\":\"两小无猜\"\n" +
//                "}").index("movie").type("_doc").id("1003").build();

        Movie movie = new Movie("0004", "泰坦尼克号");
        Index index = new Index.Builder(movie).index("movie").type("_doc").id("1004").build();

        //6.执行插入数据操作
        jc.execute(index);

        //7.关闭连接
        jc.shutdownClient();
    }
}
