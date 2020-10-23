import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.*;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Main {

    public static class MapClassId extends Mapper<LongWritable, Text, Text, Text> {
        private Text id = new Text();
        private Text person = new Text();
        HashSet<String> seen = new HashSet<>();

        @Override
        public void map(LongWritable key, Text input_line, Context context) throws IOException, InterruptedException {
            String filename = "person_id.txt";
            Writer out = new OutputStreamWriter(new FileOutputStream(filename, true), "UTF-8");

            Pattern pattern = Pattern.compile("(.*?(people.person).*)");
            String line = input_line.toString();
            Matcher matcher = pattern.matcher(line);

            if(matcher.matches()) {
                String[] tmp_triplet = line.split("\t");
                String person_id = getId(tmp_triplet[0]);

                BufferedReader br = new BufferedReader(new FileReader(filename));
                boolean find_line = false;

                String current_line;
                while ((current_line = br.readLine()) != null) {
                    if(current_line.equals(person_id)){
                        find_line = true;
                        break;
                    }
                }

                if(!find_line){
                //if(!seen.contains(person_id)){
                    out.write(person_id + "\n");
                    out.close();

                    //seen.add(person_id);

                    person.set("person");
                    id.set(person_id);
                    context.write(person, id);
                }
            }
        }

        public String getId(String base_triplet){
            String[] id = base_triplet.split("/");
            id[4] = id[4].substring(0, id[4].length() - 1);
            return id[4];
        }
    }

    public class ReduceClassId extends Reducer<Text, Text, Text, Text>{
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for (Text t : values) {
                context.write(key, t);
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf1=new Configuration();
        Job j1=Job.getInstance(conf1);
        j1.setJarByClass(Main.class);
        j1.setMapperClass(MapClassId.class);

        j1.setOutputKeyClass(Text.class);
        j1.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(j1,new Path(args[0]));
        FileOutputFormat.setOutputPath(j1, new Path(args[1]));
        j1.waitForCompletion(true);

        Configuration conf2=new Configuration();
        Job j2=Job.getInstance(conf2);
        j2.setJarByClass(Main.class);
        j2.setMapperClass(MapClass.class);
        j2.setReducerClass(ReduceClass.class);

        j2.setOutputKeyClass(Text.class);
        j2.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(j2,new Path(args[0]));
        FileOutputFormat.setOutputPath(j2,new Path(args[2]));
        System.exit(j2.waitForCompletion(true)?0:1);
    }
}