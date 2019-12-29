package com.pjm.pjm.Util;


import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;



/*

 * 继承Mapper类需要定义四个输出、输出类型泛型：

 * 四个泛型类型分别代表：

 * KeyIn        Mapper的输入数据的Key，这里是每行文字的起始位置（0,11,...）

 * ValueIn      Mapper的输入数据的Value，这里是每行文字

 * KeyOut       Mapper的输出数据的Key，这里是每行文字中的单词"hello"

 * ValueOut     Mapper的输出数据的Value，这里是每行文字中的出现的次数

 *

 * Writable接口是一个实现了序列化协议的序列化对象。

 * 在Hadoop中定义一个结构化对象都要实现Writable接口，使得该结构化对象可以序列化为字节流，字节流也可以反序列化为结构化对象。

 * LongWritable类型:Hadoop.io对Long类型的封装类型

 */


public class TestMapper extends Mapper<LongWritable, Text, Text, LongWritable> {
    public static Map<String, Object> map2 = new HashMap<>();
    int mark = 0;
    int q=0;
    int y=0;
    int n=0;
    Map<String, Object> map1 = WordMap.getmap();

    @Override

    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

//        try {
//            System.out.println("进入testmapper");
//            System.out.println(value.toString());

//            System.out.println(map.get("i"));
            Double i = Double.parseDouble(map1.get("i").toString());
            Double j = Double.parseDouble(map1.get("j").toString());
//            System.out.println(i+"+"+j);
            Double withi = i /(i + j);
            Double withj = j / (i + j);
//            Double withi = 0.5;
//            Double withj = 0.5;
            Double ipro = 1.0;
            Double jpro = 1.0;
//            System.out.println(withi+"+"+withj);
            int haoping = 0;
            int chaping = 0;
            String pattern = "[\\u4E00-\\u9FA5]+";
            String[] x = value.toString().split(" |\t");
            for (String k : x) {
                Double ishaoping = 1.0;
                Double ischaping = 1.0;
                if (Pattern.matches(pattern, k)) {
                    int goodknum=1;
                    int badknum=1;
                    if (map1.get("好评_"+k)!=null){
                       goodknum = Integer.parseInt(map1.get("好评_" + k).toString());
                     }
//                     if(map1.get("好评_"+k)==null){
//                     	goodknum =1;
//                     }
                    if (map1.get("差评_"+k)!=null){                   
                        badknum = Integer.parseInt(map1.get("差评_" + k).toString());                      
                     }
//                     if(map1.get("差评_"+k)==null){
//                     	badknum =1;
//                     }
                     ishaoping = goodknum / i;                    
                     ischaping = badknum / j; 
                     ipro=ipro*ishaoping;
                     jpro = ischaping*jpro;
                    //System.out.println(ipro+"+"+ischaping+"<<<<<<<<<<<<<<<<<<<<<<<");
//                    if (ishaoping > ischaping) {
//                        haoping++;
//                    }
//                    if (ishaoping < ischaping) {
//                        chaping++;
//                    } else {
//                        haoping++;
//                        chaping++;
//                    }
                }
            }
            ipro = ipro * withi;
            jpro = jpro * withj;
//            Boolean y = ipro > jpro;
//            System.out.println("cishu +" + mark + "  +   " + y + "<<<<<<<<<<<<<<<<<<<<<<<");
            System.out.println(map1.size() + "+" + mark + "?????????????????");

            if (x[0].equals("好评") && ipro>jpro)
            {
               y++;
            }
             if (x[0].equals("差评")&& ipro<jpro)
            {
                n++;
            }
            if (ipro > jpro) {
                context.write(value, new LongWritable(0));
                System.out.println("这是好评！！！！！！");
                map2.put(value.toString(), 0);
            }
            if (ipro < jpro) {
                context.write(value, new LongWritable(-1));
                System.out.println("这是差评！！！！！！");
                map2.put(value.toString(), -1);
            }
            else if (x[0].equals("好评")){
                //context.write(value, new LongWritable(1));
                //System.out.println("这不是！！！！！！");
                //map2.put(key.toString(),-1);
                y++;
            }
//        } catch (Exception e) {
//           e.printStackTrace();
//        }
        System.out.println(mark+"   "+y+"   "+n+"  "+q);
        mark++;
    }
}

