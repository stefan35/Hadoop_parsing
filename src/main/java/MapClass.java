import java.io.*;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MapClass extends Mapper<LongWritable, Text, Text, Text> {
    private Text id = new Text();
    private Text value = new Text();
    private ArrayList<String> person = new ArrayList<String>();
    private ArrayList<String> tmp_list = new ArrayList<String>();
    String current = "";

    @Override
    public void map(LongWritable key, Text input_line, Context context) throws IOException, InterruptedException {
        Pattern pattern = Pattern.compile("(.*?(person).*)");
        String line = input_line.toString();
        Matcher matcher = pattern.matcher(line);

        String person_id = "";
        String person_attribute = "";
        String person_value = "";
        String[] tmp_triplet = line.split("\t");
        String pi = getId(tmp_triplet[0]);

        if (tmp_list.size() < 15 && !matcher.matches())
            tmp_list.add(line);
        else if (tmp_list.size() >= 15 && !matcher.matches()){
            tmp_list.remove(0);
            tmp_list.add(line);
        }

        if(!current.equals(pi) && !current.equals(""))
        {
            value.set(String.valueOf(person));
            id.set(current);
            context.write(value, id);
            person.clear();
            current = person_id;
        }

        try{
            if (matcher.matches() || line.contains(current)){
                String[] triplets = matcher.group().split("\t");
                if(!triplets[1].contains("-rdf-syntax-") && !triplets[1].contains("type")) {
                    person_id = getId(triplets[0]);

                    if(current.equals(""))
                        current = person_id;
                    if(current.equals(person_id)){
                        checktTiplets(person_id);
                        person_attribute = getAttribute(triplets[1]);
                        person_value = getValue(triplets[2]);
                        person.add(person_attribute);
                        person.add(person_value);
                    }
                }
            }
        }catch (Exception e){
            //System.out.println(current);
        }
    }

    public void checktTiplets(String id){
        String person_id = "";
        String person_attribute = "";
        String person_value = "";

        for(int i = 0; i < tmp_list.size(); i++){
                String[] triplets = tmp_list.get(i).split("\t");
                person_id = getId(triplets[0]);
            if(person_id.equals(id) && !triplets[1].contains("-rdf-syntax-") && triplets[1].contains("object.name")){
                person_attribute = getAttribute(triplets[1]);
                person_value = getValue(triplets[2]);
                person.add(person_attribute);
                person.add(person_value);
            }
        }
        tmp_list.clear();
    }

    public String getId(String base_triplet){
        String[] id = base_triplet.split("/");
        id[4] = id[4].substring(0, id[4].length() - 1);
        return id[4];
    }

    public String getAttribute(String base_triplet){
        //String[] attribute = base_triplet.split("/");
        //String[] final_attribute = attribute[4].split("\\.");
        //final_attribute[2] = final_attribute[2].substring(0, final_attribute[2].length() - 1);
        return base_triplet;
    }

    public String getValue(String base_triplet){
        return base_triplet;
        /*String[] value = new String[0];
        if(base_triplet.contains("XMLSchema")) {
            value = base_triplet.split("\"");
            return value[1];
        }
        else if(base_triplet.contains("\"")){
            value = base_triplet.split("\"");
            return value[1];
        }
        else{
            value = base_triplet.split("/");
            value[4] = value[4].substring(0, value[4].length() - 1);
            return value[4];
        }*/
    }
}