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

    public static class MapClassId extends Mapper<LongWritable, Text, Text, IntWritable> {
        private Text id = new Text();
        private final static IntWritable one = new IntWritable(1);

        @Override
        public void map(LongWritable key, Text input_line, Context context) throws IOException, InterruptedException {
            String line = input_line.toString();
            String[] tmp_triplet = line.split("\t");

            Pattern pattern = Pattern.compile("(.*(people.person).*)");
            Matcher matcher = pattern.matcher(tmp_triplet[1]);
            Pattern link_pattern = Pattern.compile(".*((people.person)(.g|.p)).*");
            Matcher link_matcher = link_pattern.matcher(tmp_triplet[2]);

            String person_id = getId(tmp_triplet[0]);

            if(matcher.matches()) {
                id.set(person_id);
                context.write(id, one);
            }
            else if(link_matcher.find()) {
                id.set(person_id);
                context.write(id, one);
            }
        }

        public String getId(String base_triplet){
            String[] id = base_triplet.split("/");
            id[4] = id[4].substring(0, id[4].length() - 1);
            return id[4];
        }
    }

    public static class ReduceClassId extends Reducer<Text, IntWritable, Text, IntWritable>{
        Text id = new Text();
        private IntWritable value = new IntWritable(0);

        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;

            for (IntWritable value : values)
                sum += value.get();

            value.set(sum);
            context.write(key, value);
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
            ArrayList<String> final_person = new ArrayList<String>();

            Pattern person_pattern_link = Pattern.compile(".*(gender|profession).*");
            Matcher person_matcher_link = person_pattern_link.matcher(line);
            Pattern person_pattern = Pattern.compile(".*name.*");
            Matcher person_matcher = person_pattern.matcher(line);
            Pattern list_pattern = Pattern.compile("\\[([^\\]\\[]*)\\]");
            Matcher list_matcher_value = list_pattern.matcher(line);
            String[] tmp_person_link = line.split("\t");

            if(person_matcher.matches() && !person_matcher_link.matches()) {
                person.set(tmp_person_link[0]);
                value.set(tmp_person_link[1]);
                context.write(person, value);
                return;
            } else if(person_matcher_link.matches()){
                String list_person;
                ArrayList<String> prepare_link = new ArrayList<String>();

                if(list_matcher_value.find()) {
                    list_person = list_matcher_value.group(0).substring(1, list_matcher_value.group(0).length() - 1);
                    String[] attributes = list_person.split(",");
                    for (int i = 0; i < attributes.length; i++) {
                        if (!attributes[i].contains("\"")) {
                            String[] tmp_find_link = attributes[i].split(" ");
                            if (tmp_find_link.length == 2) {
                                prepare_link.add(tmp_find_link[0] + "-" + tmp_find_link[1]);
                            } else if (tmp_find_link.length == 3) {
                                prepare_link.add(tmp_find_link[1] + "-" + tmp_find_link[2]);
                            }
                        }else {
                            final_person.add(attributes[i]);
                        }
                    }
                }

                String current_line;
                try (BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(linkFile), "UTF-8"))) {
                    while ((current_line = br.readLine()) != null) {
                        String[] tmp_link_file = current_line.split("\t");
                        for(int i = 0; i < prepare_link.size(); i++){
                            if(prepare_link.get(i).contains(tmp_link_file[0])){
                                String[] match_link = prepare_link.get(i).split("-");
                                tmp_link_file[1] = tmp_link_file[1].substring(1,  tmp_link_file[1].length() - 1);
                                final_person.add(match_link[0] + " " + tmp_link_file[1]);
                            }
                        }
                    }
                } catch (Exception e) {

                }

                person.set(tmp_person_link[0]);
                value.set(String.valueOf(final_person));
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
        Text value = new Text();

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            ArrayList<String> person = new ArrayList<String>();
            ArrayList<String> name = new ArrayList<String>();
            ArrayList<String> alias = new ArrayList<String>();
            ArrayList<String> profession = new ArrayList<String>();
            ArrayList<String> gender = new ArrayList<String>();
            ArrayList<String> all = new ArrayList<String>();
            String tmp_list = "";
            String name1 = "";
            boolean first_date = false;
            boolean second_date = false;

            for (Text v : values) {
               person.add(v.toString());
            }

            for(int i = 0; i < person.size(); i++){
                String tmp = person.get(i).substring(1, person.get(i).length() - 1);
                String[] tmp_attributes = tmp.split(",");
                for(int j = 0; j < tmp_attributes.length; j++){

                    if(tmp_attributes[j].contains("name")){
                        String[] tmp_find = tmp_attributes[j].split("name");
                        name.add(tmp_find[1]);
                    }
                    else if(tmp_attributes[j].contains("alias")){
                        String[] tmp_find = tmp_attributes[j].split("alias");
                        alias.add(tmp_find[1]);
                    }
                    else if(tmp_attributes[j].contains("profession")){
                        String[] tmp_find = tmp_attributes[j].split("profession");
                        String[] attribute = tmp_find[1].split("\"");
                        profession.add(attribute[1]);
                    }
                    else if(tmp_attributes[j].contains("gender")){
                        String[] tmp_find = tmp_attributes[j].split("gender");
                        String[] attribute = tmp_find[1].split("\"");
                        gender.add(attribute[1]);
                    }
                    else if(tmp_attributes[j].contains("date_of_b")){
                        first_date = true;
                        String[] attribute = tmp_attributes[j].split("\"");
                        attribute[0] = attribute[0].replace("\"", "");
                        attribute[0] = attribute[0].replace(" ", "");
                        all.add(attribute[0] + ":" + attribute[1]);
                    }
                    else if(tmp_attributes[j].contains("date_of_d")){
                        String[] attribute = tmp_attributes[j].split("\"");
                        attribute[0] = attribute[0].replace("\"", "");
                        attribute[0] = attribute[0].replace(" ", "");
                        all.add(attribute[0] + ":" + attribute[1]);
                    }
                }
            }
            tmp_list = String.join("|", name);
            all.add("name:" + tmp_list);
            if(alias.size() == 0)
                all.add("alias:" + "NONE");
            else if(!(alias.size() == 0)) {
                tmp_list = String.join("|", alias);
                all.add("alias:" + tmp_list);
            }
            if(profession.size() == 0)
                all.add("profession:" + "NONE");
            else if(!(profession.size() == 0)) {
                tmp_list = String.join("|", profession);
                all.add("profession:" + tmp_list);
            }
            if(gender.size() == 0)
                all.add("gender:" + "NONE");
            else if(!(gender.size() == 0)) {
                tmp_list = String.join("|", gender);
                all.add("gender:" + tmp_list);
            }
            if(!first_date){
                all.add("date_of_birth:" + "NONE");
            }

            value.set(String.valueOf(all));
            context.write(key, value);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf1=new Configuration();
        Job j1=Job.getInstance(conf1);

        j1.setJarByClass(Main.class);
        j1.setMapperClass(MapClassId.class);
        j1.setReducerClass(ReduceClassId.class);
        j1.setOutputKeyClass(Text.class);
        j1.setOutputValueClass(IntWritable.class);
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

        Job j3=Job.getInstance(conf3);
        j3.setJarByClass(Main.class);
        j3.setMapperClass(MapLink.class);
        j3.setReducerClass(ReduceLink.class);
        j3.setOutputKeyClass(Text.class);
        j3.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(j3,new Path(args[4]));
        FileOutputFormat.setOutputPath(j3,new Path(args[5]));
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
        FileInputFormat.addInputPath(j4,new Path(args[4]));
        FileOutputFormat.setOutputPath(j4,new Path(args[7]));
        System.exit(j4.waitForCompletion(true)?0:1);
    }
}