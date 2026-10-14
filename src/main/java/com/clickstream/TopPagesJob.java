package com.clickstream;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.PriorityQueue;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.reduce.LongSumReducer;

/**
 * Report 2 - top-N pages.
 *
 * The Kaggle dataset has no URL column, so a "page" is a product detail page:
 * we count `view` events per product (configurable with
 * -Dclickstream.top.event=purchase to rank by purchases instead).
 *
 * Two chained jobs (the classic scalable Top-N pattern):
 *   Stage 1 (many reducers): (product, category, brand) -> count
 *   Stage 2 (one reducer)  : every mapper keeps only its local top N, the
 *                            single reducer merges those small lists.
 *
 * Final output line: "rank \t product_id \t category \t brand \t count"
 */
public final class TopPagesJob {

    public static final String CONF_TOP_N = "clickstream.topn";
    public static final String CONF_EVENT = "clickstream.top.event";

    private TopPagesJob() { }

    // ------------------------------------------------------------------ stage 1

    public static class CountMapper extends Mapper<LongWritable, Text, Text, LongWritable> {
        private static final LongWritable ONE = new LongWritable(1);
        private final Text outKey = new Text();
        private String wantedEvent;

        @Override
        protected void setup(Context ctx) {
            wantedEvent = ctx.getConfiguration().get(CONF_EVENT, "view");
        }

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
                ctx.getCounter(ClickEvent.COUNTER_GROUP, "TOPPAGES_SKIPPED_INVALID").increment(1);
                return;
            }
            if (!e.eventType.equals(wantedEvent)) {
                return;
            }
            outKey.set(e.productId + "\t" + e.categoryCode + "\t" + e.brand);
            ctx.write(outKey, ONE);
        }
    }

    // ------------------------------------------------------------------ stage 2

    /** (count, key) ordered so that the "worst" entry is the smallest. */
    static final class Entry implements Comparable<Entry> {
        final long count;
        final String key;

        Entry(long count, String key) {
            this.count = count;
            this.key = key;
        }

        @Override
        public int compareTo(Entry o) {
            if (count != o.count) {
                return Long.compare(count, o.count);   // fewer views = worse
            }
            return o.key.compareTo(key);               // tie: larger key = worse
        }
    }

    private static void offer(PriorityQueue<Entry> heap, Entry e, int n) {
        heap.add(e);
        if (heap.size() > n) {
            heap.poll();
        }
    }

    /** Input: stage-1 output lines "product \t category \t brand \t count". */
    public static class TopNMapper extends Mapper<LongWritable, Text, NullWritable, Text> {
        private final PriorityQueue<Entry> heap = new PriorityQueue<Entry>();
        private int n;

        @Override
        protected void setup(Context ctx) {
            n = ctx.getConfiguration().getInt(CONF_TOP_N, 10);
        }

        @Override
        protected void map(LongWritable offset, Text value, Context ctx) {
            String line = value.toString();
            int tab = line.lastIndexOf('\t');
            if (tab < 0) {
                return;
            }
            long count = Long.parseLong(line.substring(tab + 1).trim());
            offer(heap, new Entry(count, line.substring(0, tab)), n);
        }

        @Override
        protected void cleanup(Context ctx) throws IOException, InterruptedException {
            for (Entry e : heap) {
                ctx.write(NullWritable.get(), new Text(e.count + "\t" + e.key));
            }
        }
    }

    public static class TopNReducer extends Reducer<NullWritable, Text, Text, Text> {
        private final PriorityQueue<Entry> heap = new PriorityQueue<Entry>();
        private int n;

        @Override
        protected void setup(Context ctx) {
            n = ctx.getConfiguration().getInt(CONF_TOP_N, 10);
        }

        @Override
        protected void reduce(NullWritable k, Iterable<Text> values, Context ctx) {
            for (Text v : values) {
                String s = v.toString();            // Text objects are reused: copy via toString
                int tab = s.indexOf('\t');
                offer(heap, new Entry(Long.parseLong(s.substring(0, tab)), s.substring(tab + 1)), n);
            }
        }

        @Override
        protected void cleanup(Context ctx) throws IOException, InterruptedException {
            List<Entry> sorted = new ArrayList<Entry>(heap);
            Collections.sort(sorted, Collections.reverseOrder());   // best first
            int rank = 1;
            for (Entry e : sorted) {
                ctx.write(new Text(Integer.toString(rank++)), new Text(e.key + "\t" + e.count));
            }
        }
    }

    // ------------------------------------------------------------------ factories

    public static Job createCountJob(Configuration conf, Path in, Path out) throws IOException {
        Job job = Job.getInstance(conf, "clickstream-top-pages-1-count");
        job.setJarByClass(TopPagesJob.class);
        job.setMapperClass(CountMapper.class);
        job.setCombinerClass(LongSumReducer.class);
        job.setReducerClass(LongSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);
        FileInputFormat.addInputPath(job, in);
        FileOutputFormat.setOutputPath(job, out);
        return job;
    }

    public static Job createTopNJob(Configuration conf, Path stage1Out, Path out) throws IOException {
        Job job = Job.getInstance(conf, "clickstream-top-pages-2-topn");
        job.setJarByClass(TopPagesJob.class);
        job.setMapperClass(TopNMapper.class);
        job.setReducerClass(TopNReducer.class);
        job.setNumReduceTasks(1);
        job.setMapOutputKeyClass(NullWritable.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, stage1Out);
        FileOutputFormat.setOutputPath(job, out);
        return job;
    }
}
