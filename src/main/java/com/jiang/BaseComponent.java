package com.jiang;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

import java.io.IOException;

/**
 * @description:
 * @author:
 * @create: 2019-04-23 18:32
 */
public class BaseComponent {

    public static int BUFF_SIZE = 0x80;

    /**
     * 获取HDFS文件系统
     * @return
     */
    protected FileSystem getHDFSFileSyetem()throws IOException {
        // core-site.xml, core-default.xml, hdfd-site.xml, hdfs-default.xml
        // 解析上述配置文件的， 将core-site.xml 与 hdfs-site.xml  log4j.properties拷贝到 项目的 resources目录下
        Configuration conf  = new Configuration();
        FileSystem fs  =  FileSystem.get(conf);
        return fs;

    }


}

