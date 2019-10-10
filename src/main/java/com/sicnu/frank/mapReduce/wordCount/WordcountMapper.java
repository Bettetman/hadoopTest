package com.sicnu.frank.mapReduce.wordCount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class WordcountMapper extends Mapper<LongWritable, Text,Text, IntWritable> {

    private  Text k = new Text();
    private  IntWritable v = new IntWritable(1);
    @Override
    protected void map(LongWritable key, Text value, Context context) throws
            IOException, InterruptedException {
        //取数据
        String line = value.toString();
        String[] words = line.split(" ");
        //写数据
        for (String word:words)
        {
            //单词 输
            k.set(word);
            context.write(k,v);
        }


    }
}
