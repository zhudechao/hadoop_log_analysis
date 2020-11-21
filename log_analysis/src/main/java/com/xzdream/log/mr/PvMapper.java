package com.xzdream.log.mr;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * 统计浏览量
 */
public class PvMapper extends Mapper<LongWritable, Text,Text,LongWritable> {
    //定义一个固定的key
    private Text KEY = new Text("key");
    private LongWritable ONE = new LongWritable(1);

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //将结果写出
        context.write(KEY,ONE);
    }
}
