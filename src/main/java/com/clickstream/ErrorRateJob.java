package com.clickstream;

import java.io.IOException;
import java.util.Locale;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * Report 3 - error rate.
 *
 * The Kaggle dataset is a product-event export (no HTTP status column), so an
 * "error" is a malformed / invalid record - see ClickEvent.Problem. This is the
 * data-quality error rate of the logging pipeline. For raw web-server logs the
 * same job shape works: swap the validation in ClickEvent for "status >= 400".
 *
 * map    : record -> ("DAY|yyyy-MM-dd", "1,<0|1>")  (+ ("REASON|<problem>", "0,1") when bad)
 * combine: element-wise sum of "total,errors"
 * reduce : "key \t total \t errors \t error_rate_percent"
 */
public final class ErrorRateJob {

    private ErrorRateJob() { }

    public static class ErrorMapper extends Mapper<LongWritable, Text, Text, Text> {
        private static final Text ONE_OK = new Text("1,0");
        private static final Text ONE_BAD = new Text("1,1");
        private static final Text ONLY_BAD = new Text("0,1");
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
            String day = e.date != null ? e.date : "UNKNOWN";
            outKey.set("DAY|" + day);
            ctx.write(outKey, e.problem == null ? ONE_OK : ONE_BAD);
            if (e.problem != null) {
                outKey.set("REASON|" + e.problem.name());
                ctx.write(outKey, ONLY_BAD);
                ctx.getCounter(ClickEvent.COUNTER_GROUP, "INVALID_RECORDS").increment(1);
            }
        }
    }

    private static long[] sum(Iterable<Text> values) {
        long total = 0;
        long errors = 0;
        for (Text v : values) {
            String s = v.toString();
            int comma = s.indexOf(',');
            total += Long.parseLong(s.substring(0, comma));
            errors += Long.parseLong(s.substring(comma + 1));
        }
        return new long[] {total, errors};
    }

    public static class VectorCombiner extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context ctx)
                throws IOException, InterruptedException {
            long[] s = sum(values);
            ctx.write(key, new Text(s[0] + "," + s[1]));
        }
    }

    public static class RateReducer extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context ctx)
                throws IOException, InterruptedException {
            long[] s = sum(values);
            String rate = s[0] > 0
                    ? String.format(Locale.ROOT, "%.4f", 100.0 * s[1] / s[0])
                    : "NA";
            ctx.write(key, new Text(s[0] + "\t" + s[1] + "\t" + rate));
        }
    }

    public static Job create(Configuration conf, Path in, Path out) throws IOException {
        Job job = Job.getInstance(conf, "clickstream-error-rate");
        job.setJarByClass(ErrorRateJob.class);
        job.setMapperClass(ErrorMapper.class);
        job.setCombinerClass(VectorCombiner.class);
        job.setReducerClass(RateReducer.class);
        job.setNumReduceTasks(1);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, in);
        FileOutputFormat.setOutputPath(job, out);
        return job;
    }
}
