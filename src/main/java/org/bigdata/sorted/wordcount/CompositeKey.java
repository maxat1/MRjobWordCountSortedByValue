package org.bigdata.sorted.wordcount;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class CompositeKey implements WritableComparable {
    public String getWord() {
        return word;
    }

    public int getCount() {
        return count;
    }

    private String word;
    public void setCount(int count) {
        this.count = count;
    }

    private int count;

    public CompositeKey() {
    }

    public CompositeKey(String word, int count) {

        this.word = word;
        this.count = count;
    }
    @Override
    public void readFields(DataInput in) throws IOException {
// TODO Auto-generated method stub
        word = WritableUtils.readString(in);
        count = WritableUtils.readVInt(in);
    }

    @Override
    public void write(DataOutput out) throws IOException {
// TODO Auto-generated method stub
        WritableUtils.writeString(out, word);
        WritableUtils.writeVInt(out,count);
    }

    @Override
    public int compareTo(Object o) {
        CompositeKey ck = (CompositeKey)o;
// TODO Auto-generated method stub
        return ck.word.compareTo(word);
    }

    @Override
    public int hashCode() {
// TODO Auto-generated method stub
        return 23*word.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
// TODO Auto-generated method stub
        CompositeKey ck = (CompositeKey)obj;
        return ck.word.equals(word);
    }

    @Override
    public String toString() {
// TODO Auto-generated method stub
        return word;
    }

}