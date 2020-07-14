package com.atguigu.udtf;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.json.JSONArray;

import java.util.ArrayList;
import java.util.List;

/**
 * @author zhangjie
 * @create 2020-06-28 13:50
 * @description
 */
public class ExplodeJSONArray extends GenericUDTF {

    // 声明炸开数据的默认列名和类名
    @Override
    public StructObjectInspector initialize(StructObjectInspector argOIs) throws UDFArgumentException {

        if (argOIs.getAllStructFieldRefs().size() != 1) {
            throw new UDFArgumentException("explode_json_array只需要1个参数");
        }

        if (!"string".equals(argOIs.getAllStructFieldRefs().get(0).getFieldObjectInspector().getTypeName())) {
            throw new UDFArgumentException("json_array_to_struct_array的第1个参数应为string类型");
        }

        // 列名集合
        List<String> fieldNames = new ArrayList<>();
        fieldNames.add("action");

        // 列类型校验器集合
        List<ObjectInspector> fieldOIs = new ArrayList<>();
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);

        // 返回
        return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames, fieldOIs);
    }

    // 遍历每一行数据，做炸开操作（多次写出操作）
    // [{"a","a1"},{"b","b1"},{"c","c1"}]
    // process会将参数封装成一个数组 => [ [{"a","a1"},{"b","b1"},{"c","c1"}] ]
    public void process(Object[] objects) throws HiveException {
        if (objects.length <= 0) {
            return;
        }

        if (objects[0] == null) {
            return;
        }

        String input = objects[0].toString();

        // 对输入数据创建json数组
        JSONArray actions = new JSONArray(input);

        // 遍历json数组，将每个元素写出
        for (int i = 0; i < actions.length(); i++) {
            ArrayList<Object> results = new ArrayList<>();
            results.add(actions.getString(i));
            forward(results);
        }
    }

    public void close() throws HiveException {

    }
}
