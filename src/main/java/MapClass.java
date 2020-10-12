import java.io.*;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.google.common.collect.Lists;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MapClass extends Mapper<Object, Text, Text, IntWritable> {
    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();
    private ArrayList<ArrayList<String>> listOLists = new ArrayList<ArrayList<String>>();
    private HashMap<Integer, String> map_id_person = new HashMap<>();
    private ArrayList<String> person = new ArrayList<String>();

    public void firstData(String path) throws IOException{
        File input_file = new File(path);
        BufferedReader br = new BufferedReader(new FileReader(input_file));

        Pattern pattern = Pattern.compile("(.*?(person).*)");

        String line;
        String person_id;
        String person_attribute;
        String person_value;

        while ((line = br.readLine()) != null) {
            Matcher matcher = pattern.matcher(line);
            if (matcher.matches()){
                String[] triplets = matcher.group().split("\t");
                person_id = getId(triplets[0]);

                if(triplets[1].contains("person")) {
                    person_attribute = getAttribute(triplets[1]);
                    person_value = getValue(triplets[2]);
                }
                else
                    continue;


                if(!map_id_person.containsValue(person_id)) {
                    map_id_person.put(map_id_person.size(), person_id);
                    if(!person.isEmpty())
                        listOLists.add(new ArrayList<>(person));
                    person.clear();
                    person.add(person_attribute);
                    person.add(person_value);
                }
                else {
                    if(triplets[1].contains("person")) {
                        person_attribute = getAttribute(triplets[1]);
                        person_value = getValue(triplets[2]);
                        person.add(person_attribute);
                        person.add(person_value);
                    }
                    else
                        continue;
                }
            }
        }
        if(!person.isEmpty())
            listOLists.add(new ArrayList<>(person));
        System.out.println(listOLists);
        System.out.println(map_id_person);
    }

    public String getId(String base_triplet){
        String[] id = base_triplet.split("/");
        id[4] = id[4].substring(0, id[4].length() - 1);
        return id[4];
    }

    public String getAttribute(String base_triplet){
        String[] attribute = base_triplet.split("/");
        String[] final_attribute = attribute[4].split("\\.");
        final_attribute[2] = final_attribute[2].substring(0, final_attribute[2].length() - 1);
        return final_attribute[2];
    }

    public String getValue(String base_triplet){
        String[] value = new String[0];
        if(base_triplet.contains("XMLSchema")) {
            value = base_triplet.split("\"");
            return value[1];
        }
        else{
            value = base_triplet.split("/");
            value[4] = value[4].substring(0, value[4].length() - 1);
            return value[4];
        }
    }

    @Override
    public void map(Object key, Text input_line, Context context) throws IOException, InterruptedException {
        Pattern pattern = Pattern.compile("(.*?(person).*)");
        String line = input_line.toString();
        Matcher matcher = pattern.matcher(line);

        if (matcher.matches()){

            String[] first = matcher.group().split("\t");
            String[] second = first[0].split("/");
            second[4] = second[4].substring(0, second[4].length() - 1);
/*
            if(!map_id_person.containsValue(second[4])) {
                map_id_person.put(map_id_person.size(), second[4]);
                if(!person.isEmpty())
                    listOLists.add(new ArrayList<>(person));
                person.clear();
                person.add("a");
            }
            else {
                person.add("meno");
            }*/

            word.set(second[4]);
            context.write(word, one);
        }

        //System.out.println(context.getCurrentValue().getLength());







        //only unique values
        //set neumoznuje duplikaty, a set vratiti naspat do listu
        //System.out.println(ids.size() + "tu som");
        ///////////ine metody
        /*String v = value.toString();
        for (int i = 0; i < v.length(); i++) {
            word.set(v.substring(i, i + 1));
            context.write(word, one);
        }*/
        /*StringTokenizer itr = new StringTokenizer(value.toString(), "\n");
          while (itr.hasMoreTokens()) {
              if(itr.nextToken().matches("(.*?(person))")) {
                  word.set(itr.nextToken());
                  context.write(word, one);
              }else {
                  word.set("islooo else");
                  context.write(word, one);
              }
        }*/
    }

    /*public void getId(String p) throws IOException {
        File file = new File(p);
        BufferedReader br = new BufferedReader(new FileReader(file));

        Pattern pattern = Pattern.compile("(.*?(person).*)");

        String st;
        while ((st = br.readLine()) != null) {
            Matcher matcher = pattern.matcher(st);
            System.out.println(matcher);
        }
    }*/


    public void goCycle(){
        /*System.out.println(ids);
        Set<String> set = new HashSet<>(ids);
        ids.clear();
        ids.addAll(set);
        System.out.println(ids);
        System.out.println("aaaaaaaaaaa");
        System.out.println(whole_document.getLength());*/
    }
}