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
 * @description: MapReduce 类标准写法模板（八股文模板）
 * @author:
 * @create: 2019-04-24 17:58
 */
public class WordCountTmpl extends Configured implements Tool {

    private static String JOB_NAME = "word_count";

    // Map class
    /**
     * @description: wordcount程序的Map类  Mapper<KEY,VALUE,KEYOUT,VALUEOUT>
     *          key -> 偏移量
     *          value -> 行内容
     *          Map程序的输出数据格式就是reduce程序的输入格式
     * @author:
     * @create: 2019-04-24 17:59
     */
    static class WcMap extends Mapper<LongWritable, Text, Text, IntWritable> {


        // 每个单次出现一次计一次数
        public static IntWritable mapOutputValue = new IntWritable(1);

        private Text mapOutputKey = new Text();

        /**
         * 对于wc程序 的map阶段来讲，key为光标偏移量? value就是一行数据
         * @param key
         * @param value
         * @param context
         * @throws IOException
         * @throws InterruptedException
         */
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            // hadoop yarn          ->      <0,hadoop yarn>（map的输入）
            // hadoop mapreduce     ->      <11,hadoop mapreduce>
            // ...
            //
            // 每一行执行一次 map(); 第一行执行map()开始 ->
            // 将 "hadoop yarn" 进行分割 ->
            // 得到 "hadoop","yarn" 再将每一个单词作为 key，出现的次数作为 value 进行输出 (map的输出格式为 <Text,IntWritable>) ->
            // <hadoop, 1>（map的输出结果）
            // <yarn, 1>
            // 第二行执行map()开始 ->
            // ...

            String line = value.toString();
            StringTokenizer st = new StringTokenizer(line);
            while(st.hasMoreElements()){
                String wordValue = st.nextToken();
                mapOutputKey.set(wordValue);
                context.write(mapOutputKey, mapOutputValue);
            }
        }
    }

    // reduce class
    /**
     * @description: wordcount 程序的reduce类 reduce的输出结果是 单词(Text) 以及单词对应出现的个数(IntWritable)
     * @author:
     * @create: 2019-04-24 18:00
     */
    static class WcReduce extends Reducer<Text, IntWritable, Text, IntWritable> {

        private IntWritable mapOutputValue = new IntWritable();

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            // 对所有 map();的输出结果进行合并
            // e,g：所有map（);的输出结果如下
            // <hadoop,1>
            // <yarn,1>
            // <hbase,1>
            // <hadoop,1>

            // 那么从 map到 reduce的过程需要经过一个 shuffle(重新洗牌)的过程
            // shuffle 的过程中对所有 map();的输出 进行一个group(分组) 将相同key的value 合并到一起
            // 比如上面 map的输出结果有两个<hadoop,1>，就将这两个合并到一起，放到一个集合当中
            // e.g:
            // <hadoop,1>
            //              ---->  <hadoop,list<1,1>>  对应 reduce函数的入参 参数key -> hadoop  values -> list<1,1>
            // <hadoop,1>

            int sum = 0;
            Iterator<IntWritable> iterator = values.iterator();
            while(iterator.hasNext()){
                sum += iterator.next().get();
            }
            // 设置 reduce 输出的值
            mapOutputValue.set(sum);

            // 将 reduce 输出的 k-v 写入到 context上下文中
            context.write(key, mapOutputValue);
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
        job.setMapperClass(WcMap.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // reduce
        job.setReducerClass(WcReduce.class);
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
            int run = ToolRunner.run(conf, new WordCountTmpl(), args);
            System.exit(run);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
