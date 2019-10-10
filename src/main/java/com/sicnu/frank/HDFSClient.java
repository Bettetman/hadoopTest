package com.sicnu.frank;


import org.apache.commons.httpclient.URIException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.permission.FsPermission;


import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

public class HDFSClient {
    HDFSHelper hp = HDFSHelper.getHDFSHelper();
    public static void main(String[] args) throws IOException,Exception, URIException {
        // 1 获取文件系统
        Configuration configuration = new Configuration();
        // 配置在集群上运行
        FileSystem fs = FileSystem.get(new URI("hdfs://192.168.31.72:9000"), configuration, "frank1");
        // 2 创建目录
        fs.mkdirs(new Path("/hehe/zhang"));
        // 3 关闭资源
        fs.close();

    }
    public void testCopyFromLocalFile() throws URISyntaxException, IOException, InterruptedException {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs:192.168.31.72:9000"), conf, "frank1");
        fs.copyFromLocalFile(new Path("J:\\hello.txt"),new Path("/user/hadoop/hello.txt"));
        fs.close();

    }
    public void testFileList() throws URISyntaxException, IOException, InterruptedException {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://192.168.31.72:9000"), conf, "frank1");
        RemoteIterator<LocatedFileStatus> getList = fs.listFiles(new Path("/"), true);
        while (getList.hasNext())
        {
            LocatedFileStatus next = getList.next();
            String path = next.getPath().getName();
            FsPermission permission = next.getPermission();
            long blockSize = next.getBlockSize();
            System.out.println(path);
            System.out.println(permission);
            System.out.println(blockSize);
            BlockLocation[] blockLocations = next.getBlockLocations();
            for(BlockLocation bl:blockLocations)
            {
                String[] hosts = bl.getHosts();
                for (String host:hosts)
                {
                    System.out.println(host);
                }
            }
            System.out.println("--------");
        }
        fs.close();
    }
    public  void testListStatus()
    {
    }
    public  void putFileToHDFS()
    {}

}
