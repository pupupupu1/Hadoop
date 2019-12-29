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


public class TestMapper2 extends Mapper<LongWritable, Text, Text, LongWritable> {
    public static Map<String, Object> map2 = new HashMap<>();
    int mark = 0;
    int q=0;
    Map<String, Object> map1 = WordMap.getmap();

    @Override

    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        try {
//            System.out.println("进入testmapper");
//            System.out.println(value.toString());

//            System.out.println(map.get("i"));
            Double i = Double.parseDouble(map1.get("i").toString());
            Double j = Double.parseDouble(map1.get("j").toString());
//            System.out.println(i+"+"+j);
            Double withi = (i + 1) / (i + j + 2);
            Double withj = (j + 1) / (i + j + 2);
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
                       goodknum = Integer.parseInt(map1.get("好评_" + k).toString()) + 1;
//                        int badknum = Integer.parseInt(map1.get("差评_" + k).toString()) + 1;
                    }
                    if (map1.get("差评_"+k)!=null){
//                        goodknum = Integer.parseInt(map1.get("好评_" + k).toString()) + 1;
                       badknum = Integer.parseInt(map1.get("差评_" + k).toString()) + 1;
                    }
                    ishaoping = goodknum / i;
                    ischaping = badknum / j;
                    ipro = ishaoping;
                    jpro = ischaping;
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
            Boolean y = ipro > jpro;
//            System.out.println("cishu +" + mark + "  +   " + y + "<<<<<<<<<<<<<<<<<<<<<<<");
            System.out.println(map1.size() + "+" + mark + "?????????????????");

            if ((mark<=1000 && ipro>jpro)|(mark>1000&&ipro<jpro))
            {
                q++;
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
//            else {
//                context.write(value, new LongWritable(1));
//                System.out.println("这不是！！！！！！");
//                map2.put(key.toString(),-1);
//            }
        } catch (Exception e) {
           e.printStackTrace();
        }
        System.out.println(q+"PPPPPPPPPPPPP");
        mark++;

        // 遍历折份的内容
//
//        for (String word : words) {
//
//            // 每出现一次则在原来的基础上：+1
////            word=word+"hellohorld!";
//
//            context.write(new Text(word), new LongWritable(1));
//
//        }

    }


}

