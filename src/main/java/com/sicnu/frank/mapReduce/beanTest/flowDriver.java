package com.sicnu.frank.mapReduce.beanTest;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class flowDriver  {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        args =new String[]{"J:\\hadoop\\input\\flowCount.txt","J:\\hadoop\\output\\g1"};
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        job.setJarByClass(flowDriver.class);

        job.setMapperClass(flowMapper.class);
        job.setReducerClass(flowReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(flowBean.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(flowBean.class);

        FileInputFormat.setInputPaths(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));

        job.waitForCompletion(true);


    }
}
