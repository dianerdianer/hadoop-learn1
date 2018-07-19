package com.budou.hadoop.count;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.net.URL;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @author lzh
 * @description
 * @since 2018/5/31-AM10:42
 */
public class WordCount {
    private static  String regEx="[`~!@#$%^&*()+=|{}':;',\\[\\].<>/?~！@#￥%……&*（）——+|{}【】‘；：”“’。，、？]";
    private static Pattern p = Pattern.compile(regEx);
    private static Matcher m = null;


    /**
     * mapper task
     * 进行数据获取及处理
     */
    public static class  WordCountMapper  extends Mapper<Object, Text, Text, LongWritable>{
        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            //每次读取的是一行
            String line = value.toString();

            //===============官方版 start===============
//            StringTokenizer stringTokenizer = new StringTokenizer(line);
//            while (stringTokenizer.hasMoreTokens()){
//                //
//                context.write(new Text(stringTokenizer.nextToken()),new LongWritable(1L));
//            }
            //===============官方版 end===============

            //===============simple版 start===============
            if(StringUtils.isNotBlank(line)){
                //数据转换
//                line = line.replace(","," ");
//                line = line.replace("."," ");
//                line = line.replace("!"," ");
//                line = line.replace(";"," ");
//                line = line.replace("("," ");
//                line = line.replace(")"," ");
//                line = line.replace("!"," ");
//                line = line.replace("!"," ");
                //数据转换
                m = p.matcher(line);
                line = m.replaceAll(" ");

                String[] split = line.split(" ");
                //分别处理切割后的数据
                for(String item : split){
                    if(StringUtils.isNotBlank(item.trim())){
                        context.write(new Text(item),new LongWritable(1L));
                    }
                }
            }
            //===============simple版 end===============

        }
    }

    /**
     * reducer task
     * 进行数据汇聚和输出
     */
    public static class WordCountReducer extends Reducer<Text, LongWritable, Text, LongWritable> {
        @Override
        protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
            // 进行单词统计
            Long sum = 0L;
            for(LongWritable value : values){
                sum +=value.get();
            }
            context.write(key,new LongWritable(sum));
        }
    }


    /**
     * 运行类
     * 进行hadoop配置,指明使用的maptask个数等信息
     * 指定输入和输出路径
     * @param args
     * @throws IOException
     */
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        //使用本地hadoop
        System.setProperty("hadoop.home.dir", "/Users/bjlx/Work/developTools/hadoop-2.7.6");
        System.setProperty("HADOOP_USER_NAME", "root");



        Configuration conf = new Configuration();
        //使用以下两种方式否可以指定hdfs集群
        conf.set("fs.defaultFS", "hdfs://192.168.56.3:9000");
//        conf.set("fs.default.name", "hdfs://192.168.56.3:9000");

        //指定允许框架,并指定框架地址
//        conf.set("mapreduce.framework.name", "yarn");
//        conf.set("yarn.resourcemanager.hostanme", "mini1");
//        conf.set("yarn.resourcemanager.address", "mini1:8032");


        //获取job实例
        Job job = Job.getInstance(conf,"wordCount");
        job.setJarByClass(WordCount.class);

        //指定mapper处理程序
        job.setMapperClass(WordCountMapper.class);
        //指定reducer处理程序
        job.setReducerClass(WordCountReducer.class);

        //指定reducer task个数,默认会根据maper的输出key 的hashcode，%taskNum  决定对应的mapper数据由哪个reducer处理
        job.setNumReduceTasks(1);
        //指定数据输入format  TODO 这里是指明数据获取处理器？ 注意这里，输入和数据format，默认使用的TextXXFormat，使用错误会导致job实例化错误
        job.setInputFormatClass(TextInputFormat.class);
        //指定数据输出format  TODO 这里是指明数据输出处理器？
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        //指定输入路径
        FileInputFormat.addInputPath(job,new Path("/user/wordcount/input"));
        //指定输出路径
        FileOutputFormat.setOutputPath(job,new Path("/user/wordcount/output"));

        job.waitForCompletion(true);
        if (job.isComplete()){
            System.out.println("job 执行完了");
        }
    }
}
