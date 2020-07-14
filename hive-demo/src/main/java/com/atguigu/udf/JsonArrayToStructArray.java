package com.atguigu.udf;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ConstantObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.json.JSONArray;
import org.json.JSONObject;

import java.util.ArrayList;
import java.util.List;

/**
 * @author zhangjie
 * @create 2020-06-29 9:40
 * @description
 */
public class JsonArrayToStructArray extends GenericUDF {

    // UDF("[{},{},{}]",     a_id,item,item_type,ts（字段）,    a_id:String,item:String...（类型）)
    @Override
    public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
        if (arguments.length < 3) {
            throw new UDFArgumentException("json_array_to_struct_array需要至少3个参数");
        }

        for (int i = 0; i < arguments.length; i++) {
            if (!"string".equals(arguments[i].getTypeName())) {
                throw new UDFArgumentException("json_array_to_struct_array的第\" + (i + 1) + \"个参数应为string类型");
            }
        }

        List<String> fieldNames = new ArrayList<>();
        // 这里的属性类型是指定的List<List<类型1>>
        List<ObjectInspector> fieldOIs = new ArrayList<>();

        for (int i = 1 + (arguments.length - 1) / 2; i < arguments.length; i++) {
            if (!(arguments[i] instanceof ConstantObjectInspector)) {
                throw new UDFArgumentException("参数错误");
            }

            String field = ((ConstantObjectInspector) arguments[i]).getWritableConstantValue().toString();
            String[] split = field.split(":");
            fieldNames.add(split[0]);
            switch (split[1]) {
                case "string":
                    fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
                    break;
                case "boolean":
                    fieldOIs.add(PrimitiveObjectInspectorFactory.javaBooleanObjectInspector);
                    break;
                case "tinyint":
                    fieldOIs.add(PrimitiveObjectInspectorFactory.javaByteObjectInspector);
                    break;
                case "smallint":
                    fieldOIs.add(PrimitiveObjectInspectorFactory.javaShortObjectInspector);
                    break;
                case "int":
                    fieldOIs.add(PrimitiveObjectInspectorFactory.javaIntObjectInspector);
                    break;
                case "bigint":
                    fieldOIs.add(PrimitiveObjectInspectorFactory.javaLongObjectInspector);
                    break;
                case "float":
                    fieldOIs.add(PrimitiveObjectInspectorFactory.javaFloatObjectInspector);
                    break;
                case "double":
                    fieldOIs.add(PrimitiveObjectInspectorFactory.javaDoubleObjectInspector);
                    break;
                default:
                    throw new UDFArgumentException("json_array_to_struct_array 不支持" + split[1] + "类型");
            }
        }

        // 这里为结构体内的类型
        return ObjectInspectorFactory.getStandardListObjectInspector(ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames, fieldOIs));
    }

    // 对每一行数据进行处理
    // UDF("[{},{},{}]",     a_id,item,item_type,ts（字段）,    a_id:String,item:String...（类型）)
    @Override
    public Object evaluate(DeferredObject[] arguments) throws HiveException {

        List<List<Object>> result = new ArrayList<>();

        DeferredObject data = arguments[0];

        // 判断 "[{},{},{}]" 是否为空
        if (data.get() == null) {
            return null;
        }

        // 提取真正的数据信息==>"[{},{},{}]"
        String line = data.get().toString();
        // jsonArray => [{},{},{}]
        JSONArray jsonArray = new JSONArray(line);

        for (int i = 0; i < jsonArray.length(); i++) {
            List<Object> struct = new ArrayList<>();
            // json => {}
            JSONObject json = jsonArray.getJSONObject(i);

            // ("[{},{},{}]",     a_id,item,item_type,ts,    a_id:String,item:String...)
            for (int j = 1; j < 1 + (arguments.length - 1) / 2; j++) {
                String key = arguments[j].get().toString();
                // struct => 一个{}的所有属性的值的集合 [a_id值, item值...]
                if (json.has(key)) {
                    struct.add(json.getString(key));
                } else {
                    struct.add(null);
                }
            }

            // result => [ [a_id值1, item值1...],[a_id值2, item值2...],[...] ]
            // 这里struct不能声明为属性，因为这里添加的是引用地址，多次添加的都指向同一地址，如果清空struct内容再添加数据，则最后结果都为最后一次添加的数据
            result.add(struct);
        }
        return result;
    }

    @Override
    public String getDisplayString(String[] children) {
        return getStandardDisplayString("JsonArrayToStructArray", children, ",");
    }
}
