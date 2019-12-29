package com.pjm.pjm.Controller;


import java.io.IOException;
import java.util.Map;


import com.pjm.pjm.Util.*;
import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.fs.Path;

import org.apache.hadoop.io.LongWritable;

import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Job;

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;


@RestController
public class WordCount {

    @RequestMapping("/mapeduce")
    public String mapreduce(String hdfspath,String outputpath,String testinput,String  testoutput) throws IOException, ClassNotFoundException, InterruptedException {

        // TODO Auto-generated method stub

        // 创建job对象

        Job job = Job.getInstance(new Configuration());

        // 指定程序的入口

        job.setJarByClass(WordCount.class);


        // 指定自定义的Mapper阶段的任务处理类

        job.setMapperClass(WordMapper.class);

        job.setMapOutputKeyClass(Text.class);

        job.setMapOutputValueClass(LongWritable.class);

        // 数据HDFS文件服务器读取数据路径

        FileInputFormat.setInputPaths(job, new Path(hdfspath));


        // 指定自定义的Reducer阶段的任务处理类

        job.setReducerClass(WordReduce.class);

        // 设置最后输出结果的Key和Value的类型

        job.setOutputKeyClass(Text.class);

        job.setOutputValueClass(LongWritable.class);

        // 将计算的结果上传到HDFS服务

        FileOutputFormat.setOutputPath(job, new Path(outputpath));


        // 执行提交job方法，直到完成，参数true打印进度和详情

        job.waitForCompletion(true);
       job.cleanupProgress();
       job.killJob();

        int i=0,j=0;
        Map<String,Object> map= WordMap.getmap();
        for (Map.Entry<String, Object> entry : map.entrySet()) {
            if(entry.getKey().indexOf("好评")!=-1){
                i+=Integer.parseInt(entry.getValue().toString());
            }
            if(entry.getKey().indexOf("差评")!=-1){
                j+=Integer.parseInt(entry.getValue().toString());
            }
        }
        map.put("i",i);
        map.put("j",j);
        System.out.println("Finished"+"+"+i+"+"+j);

        Test.test(testinput,testoutput,map);
//        job = Job.getInstance(new Configuration());
//        job.setJarByClass(WordCount.class);
////
////
////        // 指定自定义的Mapper阶段的任务处理类
////
//        job.setMapperClass(TestMapper2.class);
////
//        job.setMapOutputKeyClass(Text.class);
////
//        job.setMapOutputValueClass(LongWritable.class);
////
////        // 数据HDFS文件服务器读取数据路径
////
//        FileInputFormat.setInputPaths(job, new Path(testinput));
////
////
////        // 指定自定义的Reducer阶段的任务处理类
////
//        job.setReducerClass(TestReduce.class);
////
////        // 设置最后输出结果的Key和Value的类型
////
//        job.setOutputKeyClass(String.class);
////
//        job.setOutputValueClass(String.class);
////
////        // 将计算的结果上传到HDFS服务
////
//        FileOutputFormat.setOutputPath(job, new Path(testoutput));
////
////
////        // 执行提交job方法，直到完成，参数true打印进度和详情
////
//        job.waitForCompletion(true);
        return "success";
    }
}

