package com.sicnu.frank;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

import java.net.URI;
import java.net.URL;

public class HDFSHelper {

    private static final String URL = "hdfs://192.168.31.72:9000";
    private static final String ADMIN = "frank1";
    public static Configuration conf;
    public static FileSystem fs ;

    private HDFSHelper()
    {
        if (conf==null)
            conf = new Configuration();
            try {
                fs = FileSystem.get(new URI(URL),conf,ADMIN);
            }catch (Exception e)
            {
                e.printStackTrace();
            }

    }

    public static HDFSHelper getHDFSHelper()
    {
        return new HDFSHelper();

    }

}
