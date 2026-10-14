package com.clickstream;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.reduce.LongSumReducer;

/**
 * Report 1 - peak-traffic hour.
 *
 * map    : valid event  -> (HH, 1)
 * combine: sum
 * reduce : (HH, total) for HH = 00..23, plus a final PEAK_HOUR line.
 *
 * Output lines:  "HH \t count"   and   "PEAK_HOUR \t HH \t count"
 */
public final class PeakHourJob {

    private PeakHourJob() { }

    public static class HourMapper extends Mapper<LongWritable, Text, Text, LongWritable> {
        private static final LongWritable ONE = new LongWritable(1);
        private final Text outKey = new Text();

        @Override
        protected void map(LongWritable offset, Text value, Context ctx)
                throws IOException, InterruptedException {
            if (value.getLength() == 0) {
                return;
            }
            String line = value.toString();
            if (ClickEvent.isHeader(line)) {
                return;
            }
            ClickEvent e = ClickEvent.parse(line);
            if (e.problem != null) {
                ctx.getCounter(ClickEvent.COUNTER_GROUP, "PEAKHOUR_SKIPPED_INVALID").increment(1);
                return;
            }
            outKey.set(e.hour);
            ctx.write(outKey, ONE);
        }
    }

    /** Single reducer: 24 keys at most, so it can also track the maximum. */
    public static class HourReducer extends Reducer<Text, LongWritable, Text, Text> {
        private String bestHour = null;
        private long bestCount = -1;

        @Override
        protected void reduce(Text hour, Iterable<LongWritable> values, Context ctx)
                throws IOException, InterruptedException {
            long sum = 0;
            for (LongWritable v : values) {
                sum += v.get();
            }
            ctx.write(hour, new Text(Long.toString(sum)));
            if (sum > bestCount) {          // keys arrive sorted, so ties keep the earliest hour
                bestCount = sum;
                bestHour = hour.toString();
            }
        }

        @Override
        protected void cleanup(Context ctx) throws IOException, InterruptedException {
            if (bestHour != null) {
                ctx.write(new Text("PEAK_HOUR"), new Text(bestHour + "\t" + bestCount));
            }
        }
    }

    public static Job create(Configuration conf, Path in, Path out) throws IOException {
        Job job = Job.getInstance(conf, "clickstream-peak-hour");
        job.setJarByClass(PeakHourJob.class);
        job.setMapperClass(HourMapper.class);
        job.setCombinerClass(LongSumReducer.class);
        job.setReducerClass(HourReducer.class);
        job.setNumReduceTasks(1);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, in);
        FileOutputFormat.setOutputPath(job, out);
        return job;
    }
}
