package com.atguigu;

import java.sql.*;

/**
 * @author zhangjie
 * @create 2020-07-08 9:14
 * @description
 */
public class KylinTest {
    public static void main(String[] args) throws ClassNotFoundException, SQLException {
        //Kylin_JDBC 驱动
        String KYLIN_DRIVER = "org.apache.kylin.jdbc.Driver";

        //Kylin_URL
        String KYLIN_URL = "jdbc:kylin://hadoop102:7070/order";

        //Kylin的用户名
        String KYLIN_USER = "ADMIN";

        //Kylin的密码
        String KYLIN_PASSWD = "KYLIN";

        //添加驱动信息
        Class.forName(KYLIN_DRIVER);

        // 获取连接
        Connection conn = DriverManager.getConnection(KYLIN_URL, KYLIN_USER, KYLIN_PASSWD);

        // 预编译SQL
        PreparedStatement pstat = conn.prepareStatement("select\n" +
                " dp.REGION_NAME,\n" +
                " count(*)\n" +
                "from\n" +
                " DWD_FACT_ORDER_DETAIL od\n" +
                "join\n" +
                " DWD_DIM_BASE_PROVINCE dp\n" +
                "on\n" +
                " od.PROVINCE_ID = dp.ID\n" +
                "group by\n" +
                " dp.REGION_NAME;");

        // 执行查询
        ResultSet resultSet = pstat.executeQuery();

        // 遍历打印
        while (resultSet.next()) {
            System.out.println(resultSet.getString(1) + "-" + resultSet.getString(2));
        }

        resultSet.close();
        pstat.close();
        conn.close();
    }
}
