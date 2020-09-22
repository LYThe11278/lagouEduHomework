package com.lagou.edu.homework1;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**Reducer思路：
 * Reduce 过程获取到（num，1） kv对后，将key作为value输出。定义一个递增的sortnum作为key输出。
 */
public class NumberSortReducer extends Reducer<IntWritable,IntWritable,IntWritable,IntWritable> {
    static IntWritable sortNumber = new IntWritable(1);
    @Override
    protected void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

        for (IntWritable value:values){
            //MapReduce过程会对key进行默认排序
            context.write(sortNumber,key);
            //sortNumber 递增
            sortNumber.set(sortNumber.get()+1);
        }
    }
}
