package com.clickstream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * Entry point.
 *
 *   hadoop jar clickstream-mr.jar [-D key=value ...] <all|peakhour|toppages|errorrate> <input> <outputBase>
 *
 * With "all" the three independent report jobs are submitted to YARN at the
 * same time (peak hour, top-pages stage 1, error rate); YARN's scheduler then
 * shares the queue's capacity between them. Top-pages stage 2 starts as soon as
 * stage 1 finishes.
 *
 * Output layout under <outputBase>:
 *   peak_hour/   top_pages_stage1/   top_pages/   error_rate/
 *
 * Useful -D options:
 *   -Dmapreduce.job.queuename=batch      YARN queue
 *   -Dmapreduce.job.reduces=8            reducers for the big counting stage
 *   -Dclickstream.topn=10                N for the top-pages report
 *   -Dclickstream.top.event=view         which event type defines a "page hit"
 *   -Dclickstream.overwrite=true         delete existing output dirs first (default)
 */
public class ClickstreamDriver extends Configured implements Tool {

    @Override
    public int run(String[] args) throws Exception {
        if (args.length != 3) {
            System.err.println("Usage: ClickstreamDriver [-D k=v] <all|peakhour|toppages|errorrate> <input> <outputBase>");
            return 2;
        }
        String which = args[0];
        Path in = new Path(args[1]);
        Path base = new Path(args[2]);
        Configuration conf = getConf();

        Path peakOut = new Path(base, "peak_hour");
        Path stage1Out = new Path(base, "top_pages_stage1");
        Path topOut = new Path(base, "top_pages");
        Path errOut = new Path(base, "error_rate");

        boolean runPeak = which.equals("all") || which.equals("peakhour");
        boolean runTop = which.equals("all") || which.equals("toppages");
        boolean runErr = which.equals("all") || which.equals("errorrate");
        if (!(runPeak || runTop || runErr)) {
            System.err.println("Unknown job: " + which);
            return 2;
        }

        Job peak = null;
        Job count = null;
        Job err = null;

        if (runPeak) {
            clean(conf, peakOut);
            peak = PeakHourJob.create(conf, in, peakOut);
            peak.submit();
        }
        if (runTop) {
            clean(conf, stage1Out);
            clean(conf, topOut);
            count = TopPagesJob.createCountJob(conf, in, stage1Out);
            count.submit();
        }
        if (runErr) {
            clean(conf, errOut);
            err = ErrorRateJob.create(conf, in, errOut);
            err.submit();
        }

        boolean ok = true;
        java.util.List<Job> running = new java.util.ArrayList<Job>();
        if (peak != null) running.add(peak);
        if (count != null) running.add(count);
        if (err != null) running.add(err);

        Job top = null;
        boolean stage1CountDone = false;

        while (!running.isEmpty()) {
            java.util.Iterator<Job> it = running.iterator();
            while (it.hasNext()) {
                Job j = it.next();
                if (j.isComplete()) {
                    it.remove();
                    boolean success = j.isSuccessful();
                    System.out.println("[DONE] " + j.getJobName() + " (success=" + success + ")");
                    ok &= success;
                    if (j == count && success && runTop) {
                        stage1CountDone = true;
                    }
                }
            }
            if (stage1CountDone && top == null) {
                System.out.println("[START] clickstream-top-pages-2-topn");
                top = TopPagesJob.createTopNJob(conf, stage1Out, topOut);
                top.submit();
                running.add(top);
            }
            if (!running.isEmpty()) {
                Thread.sleep(1500);
            }
        }
        return ok ? 0 : 1;
    }

    private static void clean(Configuration conf, Path p) throws java.io.IOException {
        if (conf.getBoolean("clickstream.overwrite", true)) {
            FileSystem fs = p.getFileSystem(conf);
            if (fs.exists(p)) {
                fs.delete(p, true);
            }
        }
    }

    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new Configuration(), new ClickstreamDriver(), args));
    }
}
