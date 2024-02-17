package org.bigdata.sorted.wordcount;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;


public class JobItem extends Configured implements Tool{
    public Job parseInputAndOutput(Path in, Path out) throws IOException {
        JobConf conf = new JobConf(JobItem.class);
        FileSystem fs = FileSystem.get(conf);
        if(fs.exists(out)) {
            fs.delete(out, true);
        }

        FileInputFormat.setInputPaths(conf, in);
        FileOutputFormat.setOutputPath(conf, out);


        conf.setOutputKeyClass(CompositeKey.class);
        conf.setOutputValueClass(IntWritable.class);
        conf.setMapperClass(WordMapper.class);
        conf.setCombinerClass(WordCounter.class);
        conf.setReducerClass(WordCounter.class);

        Job job = Job.getInstance(conf,"WordCount1");
        job.setGroupingComparatorClass(CompositeKeySorter.class);

        return job;
    }


    public Job group(Path in, Path out) throws IOException {
        JobConf conf = new JobConf(JobItem.class);
        FileSystem fs = FileSystem.get(conf);
        if(fs.exists(out)) {
            fs.delete(out, true);
        }

        FileInputFormat.setInputPaths(conf, in);
        FileOutputFormat.setOutputPath(conf, out);


        conf.setJobName("DescendingOccurrence");
        conf.setOutputValueClass(IntWritable.class);
        conf.setOutputKeyClass(CompositeKey.class);
        conf.setMapperClass(GrouppingMap.class);
        Job job = Job.getInstance(conf,"Grouper");
        job.setSortComparatorClass(WordCountSorter.class);
        job.setGroupingComparatorClass(CompositeKeySorter.class);

        return job;
    }
    public static void main(String[] args) throws Exception {

        int exitCode=ToolRunner.run(new JobItem(), args);
        System.exit(exitCode);

    }
    @Override
    public int run(String[] args) throws Exception {

        if(args.length < 2) {
            System.err.println("Please type the input and output Files");
            System.exit(0);
        }
        Job j = parseInputAndOutput(new Path(args[0]), new Path(args[0] + ".tmp"));
        System.out.println("Before Submition!!!!!");
        j.waitForCompletion(false);
        System.out.println("Submitted!!!!!");
        j = group(new Path(args[0] + ".tmp"), new Path(args[1]));
        return j.waitForCompletion(false) ? 0 : 1;

    }
}

