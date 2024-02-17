package org.bigdata.sorted.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;

public class WordMapper extends MapReduceBase implements Mapper<Object, Text, CompositeKey, IntWritable>{
    final private static IntWritable ONE = new IntWritable(1);
    @Override
    public void map(Object obj, Text value, OutputCollector<CompositeKey, IntWritable> collector, Reporter report) throws IOException {
        String str = value.toString();
        String[] values = str.split(" "); // Splitting into tokens
        for(String item : values) {
            if(item.trim().isEmpty()) // Skipping if empty token
                continue;
            collector.collect(new CompositeKey(item, 1), ONE);  // Writing 1 for each token in Mapper
            System.out.println(value);

        }
    }

}
















