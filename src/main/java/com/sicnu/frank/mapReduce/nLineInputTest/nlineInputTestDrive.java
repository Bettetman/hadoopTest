package com.sicnu.frank.mapReduce.nLineInputTest;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class nlineInputTestDrive {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        args =new String[]{"J:\\hadoop\\input\\nfileInput.txt","J:\\hadoop\\output\\g4"};
        Configuration conf = new Configuration();
        Job job = Job.getInstance( conf );
        NLineInputFormat.setNumLinesPerSplit( job,3 );
        job.setInputFormatClass( NLineInputFormat.class );

        job.setJarByClass( nlineInputTestDrive.class);

        job.setMapperClass( nlineInputTestMapper.class );
        job.setReducerClass( nlineInputTestReduce.class );

        job.setMapOutputKeyClass( Text.class );
        job.setMapOutputValueClass( LongWritable.class );

        job.setOutputKeyClass( Text.class );
        job.setOutputValueClass( LongWritable.class );

        FileInputFormat.setInputPaths( job,new Path( args[0] ) );
        FileOutputFormat.setOutputPath( job,new Path( args[1] ));

        job.waitForCompletion( true );

    }
}
