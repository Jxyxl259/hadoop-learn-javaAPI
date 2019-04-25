package com.jiang.hdfs;

import org.apache.hadoop.fs.*;

import java.io.IOException;
import java.io.InputStream;
import java.io.StringWriter;

/**
 * Hello world!
 *
 */
public class ReadFileFromHDFS extends BaseComponent{

    public static String path_1 = "/user/jiangbug/hadoop/wcinput/wc.txt";



    /**
     * 读取 HDFS上的文件
     * @param args
     */
    public static void main( String[] args ) {
        FileSystem fs = null;
        ReadFileFromHDFS testObj = new ReadFileFromHDFS();
        try {
            fs = testObj.getHDFSFileSyetem();
            String s = getHDFSFileAsInputStream(path_1, fs);
            System.out.println(s);
        } catch (IOException e) {
            e.printStackTrace();
        }
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
            // 使用hadoop提供的工具类直接关闭流
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
        byte[] arr = new byte[BUFF_SIZE];
        while((len = is.read(arr)) != -1){
//            String s = new String(arr);
            if(len != BUFF_SIZE){
                sw.write(new String(arr,0,len));
            }else{
                sw.write(new String(arr));
            }
        }
        return sw.toString();
    }
}
