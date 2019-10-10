package com.sicnu.frank.mapReduce.fileInputTest;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueLineRecordReader;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class fileInputTestDriver  {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        args = new String[] {"J:\\hadoop\\input\\fileInput.txt","J:\\hadoop\\output\\g2"};
        Configuration conf = new Configuration();
        conf.set( KeyValueLineRecordReader.KEY_VALUE_SEPERATOR," " );
        Job job = Job.getInstance(conf);

        job.setJarByClass(fileInputTestDriver.class);

        job.setMapperClass(fileInputTestMapper.class);
        job.setReducerClass(fileInputTestReduce.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        FileInputFormat.setInputPaths(job,new Path(args[0]));
        job.setInputFormatClass( KeyValueTextInputFormat.class);
        FileOutputFormat.setOutputPath(job,new Path(args[1]));

        job.waitForCompletion(true);


    }
}
