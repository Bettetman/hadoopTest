package com.sicnu.frank.mapReduce.nLineInputTest;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class nlineInputTestMapper  extends Mapper<LongWritable,Text, Text, LongWritable> {
    private LongWritable v = new LongWritable( 1 );
    private Text k = new Text();
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
         String keys[]=key.toString().split( " " );
         for(String getkey:keys)
         {
             k.set(getkey);
             context.write( k,v );
         }
    }
}
