package demo.formats;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;


/**
 * LineWritable
 * <p>
 * Acts as Key
 */
public class LineWritable implements WritableComparable {

    public String fileName;
    public long lineN;

    public LineWritable() {
    }

    public LineWritable(long lineN, String fileName) {
        this.fileName = fileName;
        this.lineN = lineN;
    }

    @Override
    public int compareTo(Object o) {
        if (o instanceof LineWritable) {
            LineWritable lw = (LineWritable) o;
            int cmpr = Long.compare(lineN, lw.lineN);

            if (cmpr != 0) {
                return cmpr;
            }

            return fileName.compareToIgnoreCase(lw.fileName);

        } else return 0;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeLong(lineN);
        out.writeUTF(fileName);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        lineN = in.readLong();
        fileName = in.readUTF();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        LineWritable that = (LineWritable) o;

        // Use Line Number to fetch transition on new line when comparing into reducers
        if (lineN != that.lineN) return false;
        //if (!fileName.equals(that.fileName)) return false;
        //if (line != null ? !line.equals(that.line) : that.line != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        // Use part of fileName as value for HashPartitioner
        return fileName.split("_")[0].hashCode();
    }

    @Override
    public String toString() {
        return "LineWritable{" +
                "fileName='" + fileName + '\'' +
                ", lineN=" + lineN +
                '}';
    }
}
