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
    private ArrayList<String> attributes = new ArrayList<String>();
    private ArrayList<String> tmp_attributes = new ArrayList<String>();
    private ArrayList<String> tmp_list = new ArrayList<String>();
    String current = "";
    String attribute_id = "";

    @Override
    public void map(LongWritable key, Text input_line, Context context) throws IOException, InterruptedException {
        System.out.println("b");
        /*String filename = "values.txt";
        Writer out = new OutputStreamWriter(new FileOutputStream(filename, true), "UTF-8");

        Pattern pattern = Pattern.compile("(.*?(person).*)");
        String line = input_line.toString();
        Matcher matcher = pattern.matcher(line);

        String person_id = "";
        String person_attribute = "";
        String person_value = "";
        String[] tmp_triplet = line.split("\t");
        String pi = getId(tmp_triplet[0]);

        if(matcher.matches()) {
            value.set(line);
            id.set(pi);
            context.write(value, id);
        }



        if(current.equals(""))
            current = pi;

        if (!matcher.matches()) {
            if(tmp_list.size() < 50)
                tmp_list.add(line);
            else if(tmp_list.size() >= 50) {
                tmp_list.remove(0);
                tmp_list.add(line);
            }
            if(tmp_attributes.size() < 50)
                tmp_attributes.add(line);
            else if(tmp_attributes.size() >= 50) {
                tmp_attributes.remove(0);
                tmp_attributes.add(line);
            }
        }

        if(!current.equals(pi) && !current.equals(""))
        {
            if(!person.isEmpty()) {
                for(String str : person){
                    if(str.trim().contains("name")){
                        checktTiplets(current);
                        value.set(String.valueOf(person));
                        id.set(current);
                        context.write(value, id);
                        break;
                    }
                }
                person.clear();
            }
            if(!attributes.isEmpty()){
                BufferedReader br = new BufferedReader(new FileReader(filename));
                boolean find_line = false;

                String current_line;
                while ((current_line = br.readLine()) != null) {
                    if(current_line.contains(attribute_id)){
                        find_line = true;
                    }
                }
                if(!find_line) {
                    addOther(current);
                    out.write(attributes + "+" + attribute_id + "+" + current + "\n");
                    out.close();
                }
                attribute_id = "";
                attributes.clear();
            }
            current = pi;
        }

        try{
            if (matcher.matches() || line.contains(current)){
                String[] triplets = matcher.group().split("\t");
                if(!triplets[1].contains("-rdf-syntax-") && !triplets[1].contains("type") &&
                   !triplets[2].contains("person") && !triplets[1].contains("spouse_s") &&
                   !triplets[1].contains("parents") && !triplets[1].contains("place") &&
                   !triplets[1].contains("nationality")) {
                    person_id = getId(triplets[0]);

                    if(current.equals(person_id)){
                        checktTiplets(person_id);
                        person_attribute = triplets[1];
                        person_value = triplets[2];
                        person.add(person_attribute);
                        person.add(person_value);
                    }
                }
                else if(triplets[1].contains("predicate")){
                    person_id = getId(triplets[0]);
                    addOther(person_id);
                }
            }
        }catch (Exception e){
        }*/
    }

    public void addOther(String id){
        String person_id = "";

        for(int i = 0; i < tmp_list.size(); i++) {
            String[] triplets = tmp_list.get(i).split("\t");
            person_id = getId(triplets[0]);
            if(person_id.equals(id) && triplets[2].contains("\"")){
                attributes.add(triplets[2]);
            }
            if(person_id.equals(id) && triplets[1].contains("object>") && triplets[2].contains("<")){
                attribute_id = getId(triplets[2]);
            }
            if(person_id.equals(id)){
                tmp_list.remove(i);
                i = -1;
            }
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
                person_attribute = triplets[1];
                person_value = triplets[2];
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
}