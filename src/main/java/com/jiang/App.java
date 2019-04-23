package com.jiang;

import com.sun.org.apache.bcel.internal.generic.RETURN;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;
import org.mortbay.util.IO;

import javax.xml.transform.Result;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringWriter;

/**
 * Hello world!
 *
 */
public class App {

    public static String path_1 = "/user/jiangbug/hadoop/wcinput/wc.txt";

    public static void main( String[] args ) {
        FileSystem fs = null;

        try {
            fs = getHDFSFileSyetem();
            String s = getHDFSFileAsInputStream(path_1, fs);
            System.out.println(s);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 获取HDFS文件系统
     * @return
     */
    public static FileSystem getHDFSFileSyetem()throws IOException{
        // core-site.xml, core-default.xml, hdfd-site.xml, hdfs-default.xml
        // 解析上述配置文件的， 将core-site.xml 与 hdfs-site.xml  log4j.properties拷贝到 项目的 resources目录下
        Configuration conf  = new Configuration();
        FileSystem fs  =  FileSystem.get(conf);
        return fs;

    }

    /**
     * 将HDFS文件系统中的文件以流的形式读进来
     * @return
     */
    public static String getHDFSFileAsInputStream(String filePath, FileSystem fs) {

        Path path = new Path(filePath);
        FSDataInputStream fsDataInputStream = null;
        String result = null;
        try {
            fsDataInputStream = fs.open(path);
            result = inputSteam2String(fsDataInputStream);
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            org.apache.hadoop.io.IOUtils.closeStream(fsDataInputStream);
        }

        return result;
    }


    /**
     * 将输入流转换成String字符串
     * @return
     */
    public static String inputSteam2String(InputStream is) throws IOException{
        StringWriter sw = new StringWriter();

        int len = 0;
        byte[] arr = new byte[128];
        while((len = is.read(arr)) != -1){
//            String s = new String(arr);
            if(len != 128){
                sw.write(new String(arr,0,len));
            }else{
                sw.write(new String(arr));
            }
        }
        return sw.toString();
    }
}
