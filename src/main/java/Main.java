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

    public static class MapMerge extends Mapper<LongWritable, Text, Text, Text> {
        Text person = new Text();
        Text value = new Text();

        @Override
        public void map(LongWritable key, Text input_line, Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            File linkFile = new File(conf.get("links"));
            String line = input_line.toString();
            ArrayList<String> a = new ArrayList<String>();

            Pattern person_pattern = Pattern.compile(".*(gender|profession).*");
            Matcher person_matcher = person_pattern.matcher(line);
            Pattern list_pattern = Pattern.compile("\\[([^\\]\\[]*)\\]");
            Matcher list_matcher = list_pattern.matcher(line);

            if(!person_matcher.matches()) {
                String[] tt = line.split("\t");
                person.set("final_person");
                value.set(tt[1]);
                context.write(person, value);
                return;
            }
            else {
                String list_person;
                ArrayList<String> link = new ArrayList<String>();;
                if(list_matcher.find()){
                    list_person = list_matcher.group(0).substring(1, list_matcher.group(0).length() - 1);
                    String[] attribute = list_person.split(",");
                    for (int i = 0; i < attribute.length; i++) {
                        if (!attribute[i].contains("\"")) {
                            String[] qq = attribute[i].split(" ");
                            if (qq.length == 2) {
                                link.add(qq[0] + "-" + qq[1]);
                            } else if (qq.length == 3) {
                                link.add(qq[1] + "-" + qq[2]);
                            }
                        }else {
                            a.add(attribute[i]);
                        }
                    }
                }

                String current_line;
                try (BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(linkFile), "UTF-8"))) {
                    while ((current_line = br.readLine()) != null) {
                        String[] ee = current_line.split("\t");
                        for(int i = 0; i < link.size(); i++){
                            if(link.get(i).contains(ee[0])){
                                String[] aa = link.get(i).split("-");
                                ee[1] = ee[1].replace(", ","/");
                                ee[1] = ee[1].substring(1,  ee[1].length() - 1);
                                a.add(aa[0] + " " + ee[1]);
                            }
                        }
                    }
                } catch (Exception e) {

                }
                person.set("final_person");
                value.set(String.valueOf(a));
                context.write(person, value);
            }
        }

        public String getId(String base_triplet){
            String[] id = base_triplet.split("/");
            id[4] = id[4].substring(0, id[4].length() - 1);
            return id[4];
        }
    }

    public static class ReduceMerge extends Reducer<Text, Text, Text, Text>{

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
        j1.setReducerClass(ReduceClassId.class);
        j1.setOutputKeyClass(Text.class);
        j1.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(j1,new Path(args[0]));
        FileOutputFormat.setOutputPath(j1, new Path(args[1]));
        j1.waitForCompletion(true);
        System.out.println("First job done.");

        Configuration conf2 = new Configuration();
        conf2.set("idfile", args[3]);

        Job j2=Job.getInstance(conf2);
        j2.setJarByClass(Main.class);
        j2.setMapperClass(MapClass.class);
        j2.setReducerClass(ReduceClass.class);
        j2.setOutputKeyClass(Text.class);
        j2.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(j2,new Path(args[0]));
        FileOutputFormat.setOutputPath(j2,new Path(args[2]));
        j2.waitForCompletion(true);
        System.out.println("Second job done.");

        Configuration conf3 = new Configuration();
        conf3.set("idfile", args[3]);

        Job j3=Job.getInstance(conf3);
        j3.setJarByClass(Main.class);
        j3.setMapperClass(MapLink.class);
        j3.setReducerClass(ReduceLink.class);
        j3.setOutputKeyClass(Text.class);
        j3.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(j3,new Path(args[0]));
        FileOutputFormat.setOutputPath(j3,new Path(args[4]));
        j3.waitForCompletion(true);
        System.out.println("Third job done.");

        Configuration conf4 = new Configuration();
        conf4.set("links", args[6]);

        Job j4=Job.getInstance(conf4);
        j4.setJarByClass(Main.class);
        j4.setMapperClass(MapMerge.class);
        j4.setReducerClass(ReduceMerge.class);
        j4.setOutputKeyClass(Text.class);
        j4.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(j4,new Path(args[5]));
        FileOutputFormat.setOutputPath(j4,new Path(args[7]));
        System.exit(j4.waitForCompletion(true)?0:1);
    }
}