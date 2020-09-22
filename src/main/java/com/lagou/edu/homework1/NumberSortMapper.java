package com.lagou.edu.homework1;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * todo:实现3个文件内的数字排序
 * 思路：
 * 1、先将每个文件的数据获取到，根据作业提供的文件内容，不需要切分，生成（num,1）kv对
 * 2、将所有的数据汇总到一个reduceTask中。由课程内容可以知道，MapReduce过程是对key进行默认排序。
 * 3、Reduce 过程获取到（num，1） kv对后，将key作为value输出。定义一个递增的sortnum作为key输出。
 */
//最终输出形式为(int,int)
public class NumberSortMapper extends Mapper<LongWritable, Text,IntWritable,IntWritable> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //获取到每行数据
        String numStr = value.toString();
        //将每行数据解析成Int类型
        int num = Integer.parseInt(numStr);
        //由于输出结果为<IntWritable,IntWritable>,将num和1序列化
        context.write(new IntWritable(num),new IntWritable(1));
    }
}
