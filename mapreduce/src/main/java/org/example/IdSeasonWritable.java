package org.example;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.WritableUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

// Writable containing a Text and an int
public class IdSeasonWritable implements WritableComparable<IdSeasonWritable> {
    private Text id;
    private IntWritable season;

    public IdSeasonWritable() {
        set(new Text(), new IntWritable());
    }

    public IdSeasonWritable(String id_string, int season_int){
        set(new Text(id_string), new IntWritable(season_int));
    }

    public void set(Text id_string, IntWritable season_int){
        this.id = id_string;
        this.season = season_int;
    }

    public Text getId(){
        return id;
    }

    public IntWritable getSeason(){
        return season;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        id.write(out);
        season.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        id.readFields(in);
        season.readFields(in);
    }

    @Override
    public int hashCode() {
        return id.hashCode() * 163 + season.hashCode();
    }

    @Override
    public boolean equals(Object o) {
        if (o instanceof IdSeasonWritable) {
            IdSeasonWritable other = (IdSeasonWritable) o;
            return id.equals(other.id) && season.equals(other.season);
        }
        return false;
    }

    @Override
    public String toString() {
        return id + "," + season;
    }

    @Override
    public int compareTo(IdSeasonWritable o) {
        int cmp = id.compareTo(o.id);
        if (cmp != 0) {
            return cmp;
        }
        return season.compareTo(o.season);
    }

    public static class Comparator extends WritableComparator {
        public Comparator() {
            super(IdSeasonWritable.class);
        }

        @Override
        public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
            try {
                int firstL1 = WritableUtils.decodeVIntSize(b1[s1]) + readVInt(b1, s1);
                int firstL2 = WritableUtils.decodeVIntSize(b2[s2]) + readVInt(b2, s2);
                int cmp = compareBytes(b1, s1, firstL1, b2, s2, firstL2);
                if (cmp != 0) {
                    return cmp;
                }
                return compareBytes(b1, s1 + firstL1, l1 - firstL1, b2, s2 + firstL2, l2 - firstL2);
            } catch (IOException e) {
                throw new IllegalArgumentException(e);
            }
        }
    }

    static { // Register the comparator for this class
        WritableComparator.define(IdSeasonWritable.class, new Comparator());
    }
}
