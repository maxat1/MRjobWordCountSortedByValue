package org.bigdata.sorted.charcount;

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
//        String line = value.toString();
        String line = value.toString().toLowerCase().replaceAll("[^a-zA-Z]", "");
        String  values[] = line.split("");
        for(String SingleChar : values) {
            if(SingleChar.trim().isEmpty())
                continue;
            Text charKey = new Text(SingleChar);
            IntWritable One = new IntWritable(1);
            collector.collect(new CompositeKey(charKey.toString(), 1), ONE);
            System.out.println(value);
        }
    }

}