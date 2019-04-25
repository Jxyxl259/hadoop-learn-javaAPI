package com.jiang.mapreduce.wordcount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;

/**
 * @description: MapReduce template
 * @author:
 * @create: 2019-04-24 17:58
 */
public class MapReduceTmpl extends Configured implements Tool {

    // TODO job_name
    private static String JOB_NAME = "";

    /**
     * @description: map  Mapper<KEY,VALUE,KEYOUT,VALUEOUT>
     *          TODO describe map input key/value
     *          key ->
     *          value ->
     *          Map程序的输出数据格式就是reduce程序的输入格式
     * @author:
     * @create: 2019-04-24 17:59
     */
    // TODO Map program output key/value generic-type
    static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {

        /**
         * @param context
         * @throws IOException
         * @throws InterruptedException
         */
        @Override
        public void setup(Context context) throws IOException, InterruptedException {
            // TODO 所有map开始执行前的预处理工作
        }

        /**
         * @param key
         * @param value
         * @param context
         * @throws IOException
         * @throws InterruptedException
         */
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            // TODO map 逻辑

        }

        /**
         * @param context
         * @throws IOException
         * @throws InterruptedException
         */
        @Override
        public void cleanup(Context context) throws IOException, InterruptedException {
            // TODO 所有map 任务执行结束后的收尾工作
        }
    }

    /**
     * @description:
     * @author:
     * @create: 2019-04-24 18:00
     */
    // TODO Reduce program input/output key/value generic-type
    static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {

        @Override
        public void setup(Context context) throws IOException, InterruptedException {
            // TODO 所有 reduce 开始执行前的预处理工作
        }

        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

            // TODO reduce 逻辑

        }

        @Override
        public void cleanup(Context context) throws IOException, InterruptedException {
            // TODO 所有 reduce 任务执行结束后的收尾工作
        }
    }


    /**
     * create job, driver area, overWrite the method form Tool Interface
     * @param args
     * @return
     * @throws IOException
     * @throws ClassNotFoundException
     * @throws InterruptedException
     */
    @Override
    public int run(String[] args) throws IOException, ClassNotFoundException, InterruptedException{

        // create job
        Job job = Job.getInstance(getConf(), JOB_NAME);
        job.setJobName(JOB_NAME);

        // run jar
        job.setJarByClass(this.getClass());

        // input
        Path inPath = new Path(args[0]);
        // 注意导包org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
        FileInputFormat.addInputPath(job, inPath);

        // map
        job.setMapperClass(Map.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // reduce
        job.setReducerClass(Reduce.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        // output
        Path outPath = new Path(args[1]);
        FileOutputFormat.setOutputPath(job, outPath);

        // submit job   true: show execute log under Linux
        boolean complated = job.waitForCompletion(true);

        return complated ? 0 : 1;
    }




    public static void main(String[] args) {
        //
        try {

            Configuration conf = new Configuration();
            int run = ToolRunner.run(conf, new MapReduceTmpl(), args);
            System.exit(run);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
