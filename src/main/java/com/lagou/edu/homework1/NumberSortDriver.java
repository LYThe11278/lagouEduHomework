package com.lagou.edu.homework1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class NumberSortDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
         /*
        1. 获取配置文件对象，获取job对象实例
        2. 指定程序jar的本地路径
        3. 指定Mapper/Reducer类
        4. 指定Mapper输出的kv数据类型
        5. 指定最终输出的kv数据类型
        6. 指定job处理的原始数据路径
        7. 指定job输出结果路径
        8. 提交作业
         */
//        1. 获取配置文件对象，获取job对象实例
        final Configuration conf = new Configuration();
        final Job job = Job.getInstance(conf, "NumberSort");
//        2. 指定程序jar的本地路径
        job.setJarByClass(NumberSortDriver.class);
//        3. 指定Mapper/Reducer类
        job.setMapperClass(NumberSortMapper.class);
        job.setReducerClass(NumberSortReducer.class);
//        4. 指定Mapper输出的kv数据类型
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(IntWritable.class);
//        5. 指定最终输出的kv数据类型
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(IntWritable.class);
//        6. 指定job处理的原始数据路径
        FileInputFormat.setInputPaths(job,new Path(args[0]));
//        7. 指定job输出结果路径
        FileOutputFormat.setOutputPath(job,new Path(args[1]));
//        8. 提交作业
        boolean flag = job.waitForCompletion(true);
        //jvm退出：正常退出0，非0值则是错误退出
        System.exit(flag ? 0 : 1);

    }
}
