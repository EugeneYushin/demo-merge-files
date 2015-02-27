package demo.formats;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;


/**
 * LineNumGroupComparator
 * <p>
 * Provides grouping by line number in Reduce stage
 */
public class LineNumGroupComparator extends WritableComparator {

    // Set (createInstances = true) to prevent NPEs
    public LineNumGroupComparator() {
        super(LineWritable.class, true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        if (a instanceof LineWritable && b instanceof LineWritable) {
            if (((LineWritable) a).equals((LineWritable) b)) {
                return 0;
            } else {
                return 1;
            }
        }

        return super.compare(a, b);
    }
}
