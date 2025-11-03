package org.example;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

// This can be a top-level class or a static nested class within Main
public class GoalsMatchesWritable implements Writable {
    private IntWritable goals;
    private IntWritable matches;

    // A public no-argument constructor is ESSENTIAL for Hadoop's deserialization
    public GoalsMatchesWritable() {
        this.goals = new IntWritable();
        this.matches = new IntWritable();
    }

    // A constructor for convenience
    public GoalsMatchesWritable(int goals, int matches) {
        this.goals = new IntWritable(goals);
        this.matches = new IntWritable(matches);
    }

    // Setters for reusing objects
    public void set(int goals, int matches) {
        this.goals.set(goals);
        this.matches.set(matches);
    }

    // Getters
    public IntWritable getGoals() {
        return goals;
    }

    public IntWritable getMatches() {
        return matches;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        goals.write(out);
        matches.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        goals.readFields(in);
        matches.readFields(in);
    }

    @Override
    public int hashCode() {
        return goals.hashCode() * 31 + matches.hashCode(); // Simple hash code combination
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        GoalsMatchesWritable that = (GoalsMatchesWritable) o;
        return goals.equals(that.goals) && matches.equals(that.matches);
    }

    @Override
    public String toString() {
        return goals.toString() + "," + matches.toString();
    }
}