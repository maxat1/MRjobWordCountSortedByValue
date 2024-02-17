package org.bigdata.sorted.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;
import java.util.Iterator;

public class WordCounter extends MapReduceBase implements Reducer<CompositeKey,IntWritable, CompositeKey, IntWritable>{

    @Override
    public void reduce(CompositeKey key, Iterator<IntWritable> values,
                       OutputCollector<CompositeKey, IntWritable> collector, Reporter report)
            throws IOException {
        int sum = 0;
// TODO Auto-generated method stub
        while(values.hasNext()) {
            IntWritable item = values.next();

            sum += item.get();
        }

        key.setCount(sum);
        System.out.println(key.getWord());
        collector.collect(key, new IntWritable(sum));
    }

}

