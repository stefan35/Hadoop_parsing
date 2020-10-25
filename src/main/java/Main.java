import com.sun.jersey.server.impl.model.parameter.multivalued.StringReaderProviders;
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

        @Override
        public void map(LongWritable key, Text input_line, Context context) throws IOException, InterruptedException {
            Pattern pattern = Pattern.compile("(.*?(people.person).*)");
            String line = input_line.toString();
            Matcher matcher = pattern.matcher(line);

            if(matcher.matches()) {
                String[] tmp_triplet = line.split("\t");
                String person_id = getId(tmp_triplet[0]);

                person.set("person");
                id.set(person_id);
                context.write(person, id);
            }
        }

        public String getId(String base_triplet){
            String[] id = base_triplet.split("/");
            id[4] = id[4].substring(0, id[4].length() - 1);
            return id[4];
        }
    }

    public static class ReduceClassId extends Reducer<Text, Text, Text, Text>{
        Text id = new Text();

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String actual = "";
            for (Text t : values) {
                if(actual.equals(""))
                   actual = t.toString();
                else if(!actual.equals(t.toString())){
                    id.set(actual);
                    context.write(key, id);
                    actual = t.toString();
                }
            }
            id.set(actual);
            context.write(key, id);
        }
    }

    public static class MapClassComplete extends Mapper<LongWritable, Text, Text, Text> {
        private Text id = new Text();
        private Text person = new Text();

        @Override
        public void map(LongWritable key, Text input_line, Context context) throws IOException, InterruptedException {
            Pattern pattern = Pattern.compile("(.*?(people.person).*)");
            String line = input_line.toString();
            Matcher matcher = pattern.matcher(line);

            person.set("person");
            id.set("a");
            context.write(person, id);

        }
    }

    public class ReduceClassComplete extends Reducer<Text, Text, Text, Text> {

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for (Text t : values) {
                System.out.println(t);
                context.write(key, t);
            }
            System.out.println("dalsi");
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf1=new Configuration();
        Job j1=Job.getInstance(conf1);

        j1.setJarByClass(Main.class);
        j1.setMapperClass(MapClassId.class);
        j1.setReducerClass(ReduceClassId.class);

        j1.setOutputKeyClass(Text.class);
        j1.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(j1,new Path(args[0]));
        FileOutputFormat.setOutputPath(j1, new Path(args[1]));
        j1.waitForCompletion(true);

        Configuration conf2 = new Configuration();
        conf2.set("idfile", args[3]);

        Job j2=Job.getInstance(conf2);
        //
        j2.setJarByClass(Main.class);
        j2.setMapperClass(MapClass.class);
        j2.setReducerClass(ReduceClass.class);
        //j2.setReducerClass(ReduceClassComplete.class);

        j2.setOutputKeyClass(Text.class);
        j2.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(j2,new Path(args[0]));
        FileOutputFormat.setOutputPath(j2,new Path(args[2]));
        System.exit(j2.waitForCompletion(true)?0:1);

        /*Configuration conf3 = new Configuration();
        conf3.set("idfile", args[3]);

        Job j3=Job.getInstance(conf3);
        //
        j3.setJarByClass(Main.class);
        j3.setMapperClass(MapClassComplete.class);
        j3.setReducerClass(ReduceClassComplete.class);
        //j2.setReducerClass(ReduceClassComplete.class);

        j3.setOutputKeyClass(Text.class);
        j3.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(j3,new Path(args[2]));
        FileOutputFormat.setOutputPath(j3,new Path(args[4]));
        System.exit(j3.waitForCompletion(true)?0:1);*/
    }
}