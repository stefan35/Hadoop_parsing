import java.io.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MapClass extends Mapper<LongWritable, Text, Text, Text> {
    private Text person_id = new Text();
    private Text value = new Text();
    String current = "";
    Boolean person = false;

    @Override
    public void map(LongWritable key, Text input_line, Context context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        File idfile = new File(conf.get("idfile"));
        String line = input_line.toString();

        String[] tmp_triplet = line.split("\t");
        String id = getId(tmp_triplet[0]);
        Pattern person_pattern = Pattern.compile(".*((ns.people.person)(.g|.pr)|(person.date_of)|(.object.name.)|(topic.alias)).*");
        Matcher person_matcher = person_pattern.matcher(tmp_triplet[1]);
        Pattern person_date = Pattern.compile("\"([0-9]{4})[-](0[1-9]|1[012])[-](0[1-9]|[12][0-9]|3[01])\"|\"([0-9]{4})[-](0[1-9]|1[012])\"|\"([0-9]{4})\"");
        Matcher date_matcher = person_date.matcher(line);

        if(!current.equals(id)) {
            person = false;
            current = id;
            String current_line;

            try (BufferedReader br = new BufferedReader(new FileReader(idfile))) {
                while ((current_line = br.readLine()) != null) {
                    if (current_line.contains(id)) {
                        person = true;
                        if (person_matcher.matches()) {
                            String[] attribute = tmp_triplet[1].split("\\.");
                            attribute[4] = attribute[4].substring(0, attribute[4].length() - 1);
                            if (tmp_triplet[2].contains("\"")) {
                                if (date_matcher.find()) {
                                    person_id.set(id);
                                    value.set(attribute[4] + " " + date_matcher.group(0));
                                    context.write(person_id, value);
                                } else {
                                    person_id.set(id);
                                    value.set(attribute[4] + " " + tmp_triplet[2]);
                                    context.write(person_id, value);
                                }
                            } else {
                                String[] link_value = tmp_triplet[2].split("/");
                                link_value[4] = link_value[4].substring(0, link_value[4].length() - 1);

                                person_id.set(id);
                                value.set(attribute[4] + " " + link_value[4]);
                                context.write(person_id, value);
                            }
                        }
                    }
                }
            } catch (Exception e) {

            }
        }
        else if(person && person_matcher.matches()){
            String[] attribute = tmp_triplet[1].split("\\.");
            attribute[4] = attribute[4].substring(0, attribute[4].length() - 1);
            if (tmp_triplet[2].contains("\"")) {
                if (date_matcher.find()) {
                    person_id.set(id);
                    value.set(attribute[4] + " " + date_matcher.group(0));
                    context.write(person_id, value);
                } else {
                    person_id.set(id);
                    value.set(attribute[4] + " " + tmp_triplet[2]);
                    context.write(person_id, value);
                }
            } else {
                String[] link_value = tmp_triplet[2].split("/");
                link_value[4] = link_value[4].substring(0, link_value[4].length() - 1);

                person_id.set(id);
                value.set(attribute[4] + " " + link_value[4]);
                context.write(person_id, value);
            }
        }
    }

    public String getId(String base_triplet){
        String[] id = base_triplet.split("/");
        id[4] = id[4].substring(0, id[4].length() - 1);
        return id[4];
    }

}