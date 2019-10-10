package com.sicnu.frank.mapReduce.nLineInputTest;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class nlineInputTestReduce extends Reducer<Text, LongWritable,Text, LongWritable> {
    private LongWritable v= new LongWritable( );
    @Override
    protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
        long sum = 0l;
        for(LongWritable value:values)
        {
            sum+=value.get();
        }
        v.set( sum );
        context.write(key,v);
    }
}
