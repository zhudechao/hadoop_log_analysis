package com.xzdream.log.mr;

import com.xzdream.log.log.LogParser;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Map;

public class ETLMapper extends Mapper<LongWritable, Text, NullWritable,Text> {
    private LongWritable ONE = new LongWritable(1);

    private LogParser logParser;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        logParser = new LogParser();
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String log = value.toString();

        Map<String,String> infos = logParser.parse(log);
        StringBuilder builder = new StringBuilder();
        builder.append(infos.get("province"));
        context.write(NullWritable.get(),new Text(builder.toString()));
    }
}
