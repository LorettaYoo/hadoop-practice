package org.loretta;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class MaxScoreMR {
    public static class ScoreMap extends Mapper<LongWritable, Text, Text, IntWritable> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();

            String[] data = line.split(",");
            int score = Integer.parseInt(data[1]);

            context.write(new Text(data[0]), new IntWritable(score));
        }
    }

    public static class ScoreReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int maxScore = Integer.MIN_VALUE;
            for (IntWritable score : values) {
                maxScore = Math.max(maxScore, score.get());
            }

            context.write(key, new IntWritable(maxScore));
        }
    }

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        Configuration conf = new Configuration();
//        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
//        if (otherArgs.length < 2) {
//            System.err.println("Usage: MaxScoreMR <input_path>  <output_path>");
//            System.exit(2);
//        }

//        for (String arg : args) {
//            System.out.println(arg);
//        }
        if (args.length < 2) {
            System.out.println("Usage: MaxScoreMR <input_path>  <output_path>");
            System.exit(-1);
        }

        Job job = Job.getInstance(conf, "max score");
        job.setJarByClass(MaxScoreMR.class);
        job.setMapperClass(ScoreMap.class);
        job.setCombinerClass(ScoreReducer.class);
        job.setReducerClass(ScoreReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
