package InvertedIndex;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class InvertedIndex {

    public static class Map extends Mapper<Object, Text, Text, Text> {

        private Text keyInfo = new Text(); // 存储单词和URL组合
        private Text valueInfo = new Text(); // 存储词频
        private FileSplit fileSplit; // 存储Split对象
        int a = 0;

        // 实现map函数
        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {
            // 获得<key,value>对所属的FileSplit对象
            if ((FileSplit) context.getInputSplit() != fileSplit)
                a = 0;
            fileSplit = (FileSplit) context.getInputSplit();
            a++;
            StringTokenizer itr = new StringTokenizer(value.toString());
//            System.out.println(value.toString());
            String[] set = value.toString().split(" ");
            while (itr.hasMoreTokens()) {
                // 只获取文件的名称。
                int splitIndex = fileSplit.getPath().toString().indexOf("file");
                String[] filename = fileSplit.getPath().toString().substring(splitIndex).split("/");
                keyInfo.set(itr.nextToken() + ":"
                        + filename[filename.length - 1]);
                // 词频初始化为1
                valueInfo.set("1" + "<" + a + ">");
                context.write(keyInfo, valueInfo);
            }
        }
    }

    public static class Combine extends Reducer<Text, Text, Text, Text> {
        private Text info = new Text();

        // 实现reduce函数
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            // 统计词频
            int sum = 0;
            StringBuilder position = new StringBuilder("<");
            for (Text value : values
            ) {
                sum += Integer.parseInt(value.toString().split("<")[0]);
                position.append(value.toString().split("<")[1].split(">")[0]+",");
            }
            position.append(">");
            int splitIndex = key.toString().indexOf(":");
            // 重新设置value值由URL和词频组成
            info.set(key.toString().substring(splitIndex + 1) + ":" + sum + position);
            // 重新设置key值为单词
            key.set(key.toString().substring(0, splitIndex));
            context.write(key, info);
        }
    }

    public static class Reduce extends Reducer<Text, Text, Text, Text> {
        private Text result = new Text();

        // 实现reduce函数
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            // 生成文档列表
            StringBuilder files = new StringBuilder();
            for (Text value : values) {
                files.append(value.toString()).append(";");
            }
            result.set(files.toString());

            context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();

        Job job = new Job(conf, "Inverted Index");
        job.setJarByClass(InvertedIndex.class);

        // 设置Map、Combine和Reduce处理类
        job.setMapperClass(Map.class);
        job.setCombinerClass(Combine.class);
        job.setReducerClass(Reduce.class);

        // 设置Map输出类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        // 设置Reduce输出类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // 设置输入和输出目录
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}