package com.pjm.pjm.Util;


import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Map;



/*

 * 继承Reducer类需要定义四个输出、输出类型泛型：

 * 四个泛型类型分别代表：

 * KeyIn        Reducer的输入数据的Key，这里是每行文字中的单词"hello"

 * ValueIn      Reducer的输入数据的Value，这里是每行文字中的次数

 * KeyOut       Reducer的输出数据的Key，这里是每行文字中的单词"hello"

 * ValueOut     Reducer的输出数据的Value，这里是每行文字中的出现的总次数

 */

public class TestReduce extends Reducer<Text, LongWritable, Text, LongWritable> {


    @Override
    protected void reduce(Text key, Iterable<LongWritable> values,

                          Context context) throws IOException, InterruptedException {

        // TODO Auto-generated method stub
//        System.out.println("进入reduce!!!!!");
        Map<String,Object> map=WordMap.getmap();
//        System.out.println(map.get("i")+"+"+map.get("j"));
        int x = 0;
        int y = 0;
        long sum = 0;

        for (LongWritable i : values) {

            // i.get转换成long类型

            sum += i.get();

        }
        // 输出总计结果
        context.write(key, new LongWritable(sum));
//        WordMap.settomap(key.toString(),new LongWritable(sum).toString());
//        if(key.find("好评")!=-1){
//            x+=sum;
//        }if (key.find("差评")!=-1){
//            y+=sum;
//    }
////        System.out.println(x+"+"+y);
    }

}
