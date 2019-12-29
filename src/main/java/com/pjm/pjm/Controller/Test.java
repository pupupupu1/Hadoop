package com.pjm.pjm.Controller;


import com.pjm.pjm.Util.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.Map;


public class Test {
    public static String test(String inputpath, String outputpath, Map<String, Object> map) throws IOException, ClassNotFoundException, InterruptedException {

        // TODO Auto-generated method stub

        // 创建job对象
//        Map<String,Object> map=WordMap.getmap();
        int i = 0, j = 0;
        for (Map.Entry<String, Object> entry : map.entrySet()) {
            if (entry.getKey().indexOf("好评") != -1) {
                i++;
            }
            if (entry.getKey().indexOf("差评") != -1) {
//                j+=Integer.parseInt(entry.getValue().toString());
                j++;
            }
        }
        map.put("i", i);
        map.put("j", j);
        Print.p(map.get("i") + "？？？？？？？？？？？？？？？？？？");
        Job job1 = Job.getInstance(new Configuration());

        // 指定程序的入口

        job1.setJarByClass(Test.class);


        // 指定自定义的Mapper阶段的任务处理类

        job1.setMapperClass(TestMapper.class);

        job1.setMapOutputKeyClass(Text.class);

        job1.setMapOutputValueClass(LongWritable.class);

        // 数据HDFS文件服务器读取数据路径

        FileInputFormat.setInputPaths(job1, new Path(inputpath));

        // 指定自定义的Reducer阶段的任务处理类

        job1.setReducerClass(TestReduce.class);

        // 设置最后输出结果的Key和Value的类型

        job1.setOutputKeyClass(Text.class);

        job1.setOutputValueClass(LongWritable.class);

        // 将计算的结果上传到HDFS服务

        FileOutputFormat.setOutputPath(job1, new Path(outputpath));


        // 执行提交job方法，直到完成，参数true打印进度和详情

        job1.waitForCompletion(true);
//        int i=0,j=0;
//        Map<String,String> map= WordMap.getmap();
//        for (Map.Entry<String, String> entry : map.entrySet()) {
//            if(entry.getKey().indexOf("好评")!=-1){
//                i+=Integer.parseInt(entry.getValue());
//            }
//            if(entry.getKey().indexOf("差评")!=-1){
//                j+=Integer.parseInt(entry.getValue());
//            }
//        }
//        System.out.println("Finished"+"+"+i+"+"+j);
//        System.out.println(map.get("i") + "+" + map.get("j") + ":::::::::::::::::::::::::::");
//        Double x = Double.parseDouble(map.get("i").toString());
//        Double y = Double.parseDouble(map.get("j").toString());
//        System.out.println(x / (x + y));
//        for (Map.Entry<String, Object> entry : TestMapper2.map2.entrySet()) {
//           Print.p(entry.getValue()+"+"+entry.getKey());
//        }
        return "testsuccess";
    }
}

