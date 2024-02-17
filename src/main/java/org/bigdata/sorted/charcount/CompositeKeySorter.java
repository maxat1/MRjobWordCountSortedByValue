package org.bigdata.sorted.charcount;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class CompositeKeySorter extends WritableComparator {
    public CompositeKeySorter(){
        super(CompositeKey.class, true);
    }
    @Override
    public int compare(WritableComparable a, WritableComparable b) {
// TODO Auto-generated method stub
        CompositeKey ck1 =(CompositeKey)a;
        CompositeKey ck2 =(CompositeKey)b;
/*if(ck1.getWord().equals(ck2.getWord()))
return 0;
return ck1.getCount() - ck2.getCount();*/
        return ck1.getWord().compareTo(ck2.getWord());
    }

}
