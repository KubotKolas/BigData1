package org.example;


import org.apache.hadoop.conf.Configuration;
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

//    private final Boolean DEBUG = Boolean.TRUE;
    private final Boolean DEBUG = Boolean.FALSE;

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Main(), args);
        System.exit(res);
    }

    public int run(String[] args) throws Exception {

        if(DEBUG){
            System.out.println("DEBUG: args.length = " + args.length);
            if (args.length > 0) {
                System.out.println("DEBUG: args[0] (Input Path) = " + args[0]);
            }
            if (args.length > 1) {
                System.out.println("DEBUG: args[1] (Output Path) = " + args[1]);
            }
        }

        Configuration conf = getConf();

        if (DEBUG){

            conf.set("mapreduce.framework.name", "local");
            conf.set("fs.defaultFS", "file:///");

        }

        Job job = Job.getInstance(conf, "Project1");
        job.setJarByClass(this.getClass());
        FileInputFormat.addInputPath(job, new Path(args[1]));
        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        job.setMapperClass(FootballMapper.class);
        job.setCombinerClass(FootballCombiner.class);
        job.setReducerClass(FootballReducer.class);

        job.setMapOutputKeyClass(IdSeasonWritable.class);
        job.setMapOutputValueClass(GoalsMatchesWritable.class);


        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static class FootballMapper extends Mapper<LongWritable, Text, IdSeasonWritable, GoalsMatchesWritable> {


        private IntWritable season = new IntWritable();
        private Text home_id = new Text();
        private Text away_id = new Text();
        private IntWritable home_goals = new IntWritable();
        private IntWritable away_goals = new IntWritable();
        private IdSeasonWritable key_out_home = new IdSeasonWritable();
        private GoalsMatchesWritable val_out_home = new GoalsMatchesWritable();
        private IdSeasonWritable key_out_away = new IdSeasonWritable();
        private GoalsMatchesWritable val_out_away = new GoalsMatchesWritable();


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
                    key_out_home.set(home_id, season);
                    val_out_home.set(home_goals.get(), 1);
                    context.write(key_out_home, val_out_home);
                    key_out_away.set(home_id, season);
                    val_out_away.set(away_goals.get(), 1);
                    context.write(key_out_away, val_out_away);
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    public static class FootballReducer extends Reducer<IdSeasonWritable, GoalsMatchesWritable, Text, Text> {

        private final Text val_out = new Text();
        private final Text key_out = new Text();
        Float average;
        Float count;
        int sum;


        public void reduce(IdSeasonWritable key, Iterable<GoalsMatchesWritable> values,
                           Context context) throws IOException, InterruptedException {
            average = 0f;
            count = 0f;
            sum = 0;

            key_out.set(key.getId().toString() + '\t' + key.getSeason().toString());

            for (GoalsMatchesWritable val : values){ // Changed from IntArrayWritable
                sum += val.getGoals().get();
                count += val.getMatches().get();
            }

            average = sum/count;

            val_out.set(count.toString()+'\t'+average.toString());

            context.write(key_out, val_out);

        }
    }

    public static class FootballCombiner extends Reducer<IdSeasonWritable, GoalsMatchesWritable, IdSeasonWritable, GoalsMatchesWritable> {

        private GoalsMatchesWritable combined_out = new GoalsMatchesWritable();


        @Override
        public void reduce(IdSeasonWritable key, Iterable<GoalsMatchesWritable> values, Context context) throws IOException, InterruptedException {

            int currentGoals = 0; // Local variables for accumulation
            int currentMatches = 0;

            for (GoalsMatchesWritable val : values){ // Changed from IntArrayWritable
                currentGoals += val.getGoals().get();
                currentMatches += val.getMatches().get();
            }

            combined_out.set(currentGoals, currentMatches);

            context.write(key, combined_out);
        }
    }
}