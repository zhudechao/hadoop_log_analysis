package com.xzdream.log.mr;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * 浏览量统计
 */
public class PVReduce extends Reducer<Text, LongWritable, NullWritable,LongWritable> {
    @Override
    protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
        long count = 0;

        for (LongWritable value:values){
            count++;
        }

        context.write(NullWritable.get(),new LongWritable(count));
    }
}
