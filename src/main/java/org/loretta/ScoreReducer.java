package org.loretta;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class ScoreReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int maxScore = Integer.MIN_VALUE;
        for (IntWritable score : values) {
            maxScore = Math.max(maxScore, score.get());
        }

        context.write(key, new IntWritable(maxScore));
    }
}
