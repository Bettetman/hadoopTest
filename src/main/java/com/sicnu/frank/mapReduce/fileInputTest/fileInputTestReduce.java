package com.sicnu.frank.mapReduce.fileInputTest;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class fileInputTestReduce extends Reducer<Text, LongWritable,Text,LongWritable> {
    private LongWritable t = new LongWritable();
    @Override
    protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
        Long sum = 0l;
        for (LongWritable value:values)
        {
            sum+=value.get();
        }
        t.set(sum);
        context.write(key,t);
    }
}
