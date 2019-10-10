package com.sicnu.frank.mapReduce.beanTest;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class flowMapper  extends Mapper<LongWritable, Text,Text,flowBean> {
    private Text k =new Text();
    private flowBean v = new flowBean();
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line  = value.toString();
        String[] words = line.split("\t");
        k.set(words[1]);

        long upFlow = Long.parseLong(words[words.length-3]);
        long downFlow = Long.parseLong(words[words.length-2]);
        v.setUpFlow(upFlow);
        v.setDownFlow(downFlow);
        v.setSumFlow(upFlow+downFlow);

        context.write(k,v);
    }
}
