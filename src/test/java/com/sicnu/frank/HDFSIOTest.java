package com.sicnu.frank;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.junit.Test;

import java.io.*;

import static org.junit.Assert.*;

public class HDFSIOTest {
    HDFSHelper hp = HDFSHelper.getHDFSHelper();

    @Test
    public void putFileToHDFS() throws IOException {
        //文件上传
        FileInputStream fis = new FileInputStream(new File("J:\\hadoop\\input\\hello.txt"));
        FSDataOutputStream fos = hp.fs.create(new Path("/hello.txt"));
        IOUtils.copyBytes(fis,fos,hp.conf);
        IOUtils.closeStream(fos);
        IOUtils.closeStream(fis);

    }

    @Test
    public void downloadFile() throws IOException {
        //下载大于128m分区块的demo
        FSDataInputStream fsi = hp.fs.open(new Path("/hadoop-2.7.2.tar.gz"));
        FileOutputStream fso = new FileOutputStream(new File("J:\\file\\hadoop-2.7.2.tar.gz"));
        //流的对拷
        byte[] buff =new byte[1024];
        for (int i=0;i<1024*128;i++)
        {
            fsi.read(buff);
            fso.write(buff);
        }
        IOUtils.closeStream(fsi);
        IOUtils.closeStream(fso);
    }

    @Test
    public void downloadFilemore128() throws IOException {
        FSDataInputStream fsi = hp.fs.open(new Path("/hadoop-2.7.2.tar.gz"));
        fsi.seek(128*1024*1024);
        FileOutputStream fso = new FileOutputStream(new File("J:\\file\\hadoop-2.7.2.tar.gz2"));
        IOUtils.copyBytes(fsi,fso,hp.conf);
        IOUtils.closeStream(fso);
        IOUtils.closeStream(fsi);

    }
}