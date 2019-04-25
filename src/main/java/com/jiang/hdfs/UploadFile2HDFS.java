package com.jiang.hdfs;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;

/**
 * 上传文件到HDFS
 */
public class UploadFile2HDFS extends BaseComponent{

    /**
     * HDFS上的  目的文件路径 + 文件名
     */
    public static String dest_file = "/user/jiangbug/hadoop/test/fileupload/wc_count.txt";

    /**
     * 本地文件
     */
    public static String local_file = "/data/test/wc_count.txt";


    /**
     * check file on HDFS
     * bin ]# ./hdfs dfs -cat /user/jiangbug/hadoop/test/fileupload/wc_count.txt
     * after method executed successfully
     * @param args
     */
    public static void main(String[] args) {

        UploadFile2HDFS testObj = new UploadFile2HDFS();
        FSDataOutputStream fsDataOutputStream = null;
        FileInputStream fis = null;
        FileSystem fs = null;

        try {
            // 获取到hdfs 文件系统对象
            fs = testObj.getHDFSFileSyetem();
            // 创建 path对象
            Path p = new Path(dest_file);
            // 创建数据输出流
            fsDataOutputStream = fs.create(p);
            // 将本地文件读入内存
            fis = new FileInputStream(new File(local_file));

            // 上传
            int len = 0;
            byte[] arr = new byte[BUFF_SIZE];
            while((len = fis.read(arr)) != -1){
                if(len != BUFF_SIZE){
                    fsDataOutputStream.write(arr, 0, arr.length);
                }else{
                    fsDataOutputStream.write(arr);
                }
            }
            System.out.println("file upload task already done...");
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                if(fsDataOutputStream != null){
                    fsDataOutputStream.close();
                }
                if(fis != null){
                    fis.close();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

    }

}
