package org.example;


import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

public class Main extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Main(), args);
        System.exit(res);
    }

    public int run(String[] args) throws Exception {
        Job job = Job.getInstance(getConf(), "Project1");
        job.setJarByClass(this.getClass());
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.setMapperClass(FootballMapper.class);
        job.setCombinerClass(FootballCombiner.class);
        job.setReducerClass(FootballReducer.class);

        job.setMapOutputKeyClass(ArrayWritable.class);
        job.setMapOutputValueClass(ArrayWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static class FootballMapper extends Mapper<LongWritable, Text, ArrayWritable, ArrayWritable> {


        private IntWritable season = new IntWritable();
        private Text home_id = new Text();
        private Text away_id = new Text();
        private IntWritable home_goals = new IntWritable();
        private IntWritable away_goals = new IntWritable();
        private ArrayWritable key_out_home = new ArrayWritable(Text.class);
        private ArrayWritable val_out_home = new ArrayWritable(IntWritable.class);
        private ArrayWritable key_out_away = new ArrayWritable(Text.class);
        private ArrayWritable val_out_away = new ArrayWritable(IntWritable.class);


//          match_id,home_team_id,away_team_id,home_score,away_score,date,attendance
//          3bed22e7-affe-4f4a-afe4-8cd263dca57e,e1c8a26d-aad6-4ced-987b-6f6cf382102f,908cdc3f-1ae4-481a-a99c-ecde60da4359,0,4,2018-03-19T16:15,26376
        public void map(LongWritable offset, Text lineText, Context context) {
            try {
                if (offset.get() != 0) {
                    String line = lineText.toString();
                    int i = 0;
                    for (String word : line
                            .split(",")) {
                        if (i == 1) {
                            home_id.set(word);
                        }
                        if (i==2) {
                            away_id.set(word);
                        }
                        if (i == 3) {
                            home_goals.set(Integer.parseInt(word));
                        }
                        if (i == 4) {
                            away_goals.set(Integer.parseInt(word));
                        }
                        if (i == 5) {
                            //data
                            int t = Integer.parseInt(word.split("-")[0]);
                            if (Integer.parseInt(word.split("-")[1]) >= 8){
                                t += 1;
                            }
                            season.set(t);
                        }
                        i++;
                    }
                    //TODO: write intermediate pair to the context
                    key_out_home.set(new Text[]{home_id, new Text(season.toString())});
                    val_out_home.set(new IntWritable[]{home_goals, new IntWritable(1)});
                    context.write(key_out_home, val_out_home);
                    key_out_away.set(new Text[]{away_id, new Text(season.toString())});
                    val_out_away.set(new IntWritable[]{away_goals, new IntWritable(1)});
                    context.write(key_out_away, val_out_away);
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    public static class FootballReducer extends Reducer<ArrayWritable, ArrayWritable, Text, Text> {

        private final Text val_out = new Text();
        private final Text key_out = new Text();
        Float average;
        Float count;
        int sum;


        public void reduce(Iterable<ArrayWritable> key, Iterable<ArrayWritable> values,
                           Context context) throws IOException, InterruptedException {
            average = 0f;
            count = 0f;
            sum = 0;

            for ( ArrayWritable arr : key){
                Writable[] w = arr.get();

                key_out.set(w[0].toString()+','+w[1].toString()+',');
            }

            for ( ArrayWritable arr : values){
                Writable[] w = arr.get();
                sum += ((IntWritable) w[0]).get();
                count += ((IntWritable) w[1]).get();
            }

            average = sum/count;

            val_out.set(count.toString()+','+average.toString());

            context.write(key_out, val_out);

        }
    }

    public static class FootballCombiner extends Reducer<ArrayWritable, ArrayWritable, ArrayWritable, ArrayWritable> {

        private int goals = 0;
        private int matches = 0;

        @Override
        public void reduce(ArrayWritable key, Iterable<ArrayWritable> values, Context context) throws IOException, InterruptedException {


           for ( ArrayWritable arr : values){
               Writable[] w = arr.get();
               goals += ((IntWritable) w[0]).get();
               matches += ((IntWritable) w[1]).get();
           }

           context.write(key, new ArrayWritable(IntWritable.class, new IntWritable[]{new IntWritable(goals), new IntWritable(matches)}));
        }
    }
}