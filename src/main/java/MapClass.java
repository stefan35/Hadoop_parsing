import java.io.*;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MapClass extends Mapper<LongWritable, Text, Text, Text> {
    private Text text_id = new Text();
    private Text value = new Text();
    private ArrayList<String> person = new ArrayList<String>();
    private ArrayList<String> attributes = new ArrayList<String>();
    private ArrayList<String> tmp_attributes = new ArrayList<String>();
    private ArrayList<String> tmp_list = new ArrayList<String>();
    String current = "";
    String attribute_id = "";

    @Override
    public void map(LongWritable key, Text input_line, Context context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        File idfile = new File(conf.get("idfile"));
        String line = input_line.toString();

        String[] tmp_triplet = line.split("\t");
        String id = getId(tmp_triplet[0]);
        Pattern person_pattern = Pattern.compile(".*((ns.people.person)(.g|.pr)|(.object.name.)|date_of).*");
        Matcher person_matcher = person_pattern.matcher(line);
        Pattern link_pattern = Pattern.compile(".*(notable_for)(.display|.object).*");
        Matcher link_matcher = link_pattern.matcher(line);

        //pamatat si id
        //potom preskocit
        String current_line;
        try(BufferedReader br = new BufferedReader(new FileReader(idfile))) {
            while ((current_line = br.readLine()) != null) {
                if (current_line.contains(id) && person_matcher.matches()) {
                    //persony + atributy + hodnota
                    text_id.set(id);
                    value.set(tmp_triplet[1] + " " + tmp_triplet[2]);
                    context.write(text_id, value);
                    break;
                } else if (current_line.contains(id) && link_matcher.matches()) {
                    //hodnoty
                    if (tmp_triplet[2].contains("\"")) {
                        text_id.set(id);
                        value.set(tmp_triplet[2]);
                        context.write(text_id, value);
                    } else {
                        String[] link_value = tmp_triplet[2].split("/");
                        link_value[4] = link_value[4].substring(0, link_value[4].length() - 1);

                        text_id.set(id);
                        value.set(link_value[4]);
                        context.write(text_id, value);
                    }
                    break;
                }
            }
        }catch (Exception e){

        }
    }

    public String getId(String base_triplet){
        String[] id = base_triplet.split("/");
        id[4] = id[4].substring(0, id[4].length() - 1);
        return id[4];
    }
}