package mainclasses;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.ml.feature.StopWordsRemover;
import org.apache.spark.sql.*;
import org.apache.spark.sql.api.java.UDF3;
import org.apache.spark.sql.types.DataTypes;
import org.xml.sax.SAXException;
import scala.Tuple2;
import javax.xml.parsers.ParserConfigurationException;
import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import scala.collection.Seq;
import static org.apache.spark.sql.functions.*;

//implements AutoCloseable
public class App {

    //private final Driver driver;
    public static String get_word_toremove(String text, String pattern) {
        String p = pattern;
        String new_text = text;

        Pattern r = Pattern.compile(p);
        Matcher m = r.matcher(new_text);

        while (m.find()) {
            String found = m.group();
            new_text = new_text.replace(found, "");
        }

        return new_text;
    }

    public static HashSet<String> getLinks(String text) {
        HashSet<String> links = new HashSet<String>();
        String pattern = "\\[\\[(.*?)\\]\\]";
        Pattern r = Pattern.compile(pattern);
        Matcher m = r.matcher(text);
        while (m.find()) {
            String word = m.group(1);
            //splitting the link with | , the first element is the name of the Wikipedia page, the second element, if there is, is the word that appear in the text
            String[] part_of_word = word.split("\\|");
            //split by # to obtain the real page of the link
            String[] first_pow = part_of_word[0].split("#");
            //remove Category: from links
            String[] no_category = first_pow[0].split(":");
            if (!no_category[0].equals("category")) {
                no_category[0].replace(" ", "_");
                links.add(no_category[0].toLowerCase());

            }
        }
        return links;
    }

    public static HashSet<String> getCategories(String text) {
        HashSet<String> categories = new HashSet<>();
        String pattern = "\\[\\[Category:(.*?)\\]\\]";
        Pattern r = Pattern.compile(pattern);
        Matcher m = r.matcher(text);
        while (m.find()) {
            String word = m.group(1);
            int index = word.indexOf("|");
            if (index == -1) {
                categories.add(word);
            } else {
                String pattern2 = "(.*)\\|";
                Pattern r2 = Pattern.compile(pattern2);
                Matcher m2 = r2.matcher(word);
                while (m2.find()) {
                    String word2 = m2.group(1);
                    categories.add(word2);
                }
            }
        }
        return categories;
    }

    public static String remove_chars(String text, String[] char_to_remove) {
        String new_text = text;
        for (String c : char_to_remove) {
            new_text = new_text.replace(c, " ");
        }
        return new_text;
    }

    public static List<String> convert_map(HashMap<String, Integer> wc) {
        List<String> lmap = new ArrayList<>();
        for (Map.Entry<String, Integer> entry : wc.entrySet()) {
            String element = entry.getKey() + ":" + entry.getValue();
            lmap.add(element);
        }

        return lmap;
    }


    private static RowWikipedia setRow(Integer index, List<String> titles, List<String> k, List<HashSet<String>> links_final, List<HashSet<String>> cat_final, List<List<String>> wordcount_maps) {
        RowWikipedia new_row = new RowWikipedia();
        if (titles.size() > 0) {
            String title = titles.get(index);
            HashSet<String> links = links_final.get(index);
            HashSet<String> cat = cat_final.get(index);
            List<String> wc = wordcount_maps.get(index);
            String year = k.get(index);

            new_row.setTitle(title);
            new_row.setYear(year);
            new_row.setLinks(links);
            new_row.setCategories(new ArrayList<>(cat));
            new_row.setWordcount(wc);
        }
        return new_row;

    }

    public static void main(String[] args) throws IOException, SAXException, ParserConfigurationException, InterruptedException, URISyntaxException, Exception {
        SparkSession spark = SparkSession.builder().appName("BigData Project").getOrCreate();
        JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());


        SQLContext sqlContext = new SQLContext(sc);
        //args[0] -> input path hdfs
        String path = args[0];
        List<String> subpaths = new ArrayList<>();


        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(new URI(path), conf);
        FileStatus[] fileStatus = fs.listStatus(new Path(path));
        for (FileStatus status : fileStatus) {
            subpaths.add(status.getPath().toString());
        }

        UDF3 concatItems = new UDF3<Seq<String>, Seq<String>, Seq<String>, ArrayList<String>>() {
            public ArrayList<String> call(final Seq<String> col1, final Seq<String> col2, final Seq<String> col3) throws Exception {
                ArrayList zipped = new ArrayList();

                for (int i = 0, listSize = col1.size(); i < listSize; i++) {
                    String subRow = col1.apply(i) + "&&&" + col2.apply(i) + "&&&" + col3.apply(i);
                    zipped.add(subRow);
                }

                return zipped;
            }
        };

        //Function registration
        spark.udf().register("concatItems", concatItems, DataTypes.createArrayType(DataTypes.StringType));

        String all_pattern = "(\\[\\[(.*?)\\]\\])|(\\<ref\\>(.*?)\\<\\/ref\\>)|(\\<math\\>(.*?)\\<\\/math\\>)|(\\`\\'\\'\\'(.*?)\\'\\'\\'\\`)|(\\=\\=(.*?)\\=\\=)|(\\[(.*?)\\])|(\\'\\'\\'(.*?)\\'\\'\\')|(\\{\\{(.*?)\\}\\})";
        String[] char_to_remove = {".", ",", "(", ")", "[", "]", "\n", "{", "}", ";", "!", "?", "\"", "#", ":", "+", "*", "=", "/", "'", "\\", "~", "|", "<", ">", "_"};

        for (String subpath : subpaths) {
            System.out.println("FILE INPUT: " + subpath);

            Dataset<Row> df = sqlContext.read().format("com.databricks.spark.xml").option("rowTag", "page").option("excludeAttribute", "true").load(subpath);
            Dataset<Row> filt = df.select("title", "revision.contributor.username", "revision.timestamp", "revision.text");


            //dataset with columns title, Revision_timestamp(username, timestamp, text)
            Dataset<Row> rev_timestamp = filt.select(col("title"), callUDF("concatItems", col("username"), col("timestamp"), col("text")).alias("Revision_timestamp"));
            //dataset with one couple (username, timestamp) per line
            Dataset<Row> to_split = rev_timestamp.select(col("title"), explode(col("Revision_timestamp")).alias("Splitted"));


            //dataset with column title, Username, Year, Timestamp, Text
            Dataset<Row> final_df = to_split.selectExpr("title", "split(Splitted, '&&&')[0] as Username", "split(Splitted, '&&&')[1] as Timestamp", "split(Splitted, '&&&')[2] as Text")
                    .selectExpr("title", "Username", "split(Timestamp, '-')[0] as Year", "Timestamp", "Text", "split(lower(Text), ' ') as ArrayText");


            List<String> years = new ArrayList<>();
            years = final_df.select(col("Year")).dropDuplicates().sort().as(Encoders.STRING()).collectAsList();


            for (String year : years) {
                String mkdir_hdfs = new String();
                if (args[0].substring(0, 3).equals("gs:")) {
                    mkdir_hdfs = "gsutil mkdir " + args[0] + "/" + year + "/";
                } else if (args[0].substring(0, 9).equals("hdfs://sp")) {
                    mkdir_hdfs = "hdfs dfs -mkdir " + args[1] + "/" + year + "/";
                } else if (args[0].substring(0, 9).equals("hdfs://lo")) {
                    mkdir_hdfs = args[2] + " dfs -mkdir " + args[1] + "/" + year + "/";

                }

                if (!fs.exists(new Path(args[1] + "/" + year + "/"))) {
                    Runtime run = Runtime.getRuntime();
                    Process pr = run.exec(mkdir_hdfs);
                    pr.waitFor();
                    BufferedReader buf = new BufferedReader(new InputStreamReader(pr.getInputStream()));
                    String line = "";
                    while ((line = buf.readLine()) != null) {
                        System.out.println(line);
                    }
                }
            }

            Dataset<Row> grouped = final_df.select("title", "Year", "Text", "Timestamp").groupBy(col("title"), col("Year")).agg(functions.max("timestamp").as("Timestamp"));
            Dataset<Row> joined = final_df.select(col("text"), col("Timestamp"), col("ArrayText")).join(grouped, "Timestamp");
            joined.show();

            StopWordsRemover remover = new StopWordsRemover()
                    .setInputCol("ArrayText")
                    .setOutputCol("FilteredText");


            joined = remover.transform(joined);

            joined.printSchema();


            JavaRDD<String> texts = joined.toJavaRDD().map(row -> row.getString(1));
            List<String> titles = joined.toJavaRDD().map(row -> row.getString(3).toLowerCase()).collect();
            List<String> k = joined.toJavaRDD().map(row -> row.getString(4)).collect();
            JavaRDD<String> filterd_texts = joined.toJavaRDD().map(row -> new ArrayList<String>(row.getList(5)))
                    .map(text -> String.join(" ", text));

            JavaPairRDD<HashSet<String>, Long> links = texts.map(text -> getLinks(text)).zipWithIndex();
            List<HashSet<String>> links_f = links.map(link -> link._1()).collect();


            JavaPairRDD<HashSet<String>, Long> categories = texts.map(text -> getCategories(text)).zipWithIndex();
            List<HashSet<String>> cat_f = categories.map(cat -> cat._1()).collect();

            JavaRDD<String> new_text = filterd_texts.map(filt_t -> get_word_toremove(filt_t, all_pattern)).map(removed -> remove_chars(removed, char_to_remove));

            ArrayList<JavaRDD<String>> all_word_text_tmp = new ArrayList<>();
            for (String t : new_text.collect()) {
                all_word_text_tmp.add(sc.parallelize(new ArrayList<>(Arrays.asList(t.split(" ")))));
            }
            List<JavaPairRDD<String, Integer>> wordcount_text_list = new ArrayList<>();
            for (JavaRDD<String> t : all_word_text_tmp) {
                JavaPairRDD<String, Integer> counts;
                counts = t.flatMap(s -> Arrays.asList(s.toString().split(" ")).iterator())
                        .mapToPair(word -> new Tuple2<>(word, 1))
                        .reduceByKey((a, b) -> a + b)
                        .filter(count -> count._1() != "");
                wordcount_text_list.add(counts);
            }
            List<HashMap<String, Integer>> wordcounts_maps = new ArrayList<>();
            for (JavaPairRDD<String, Integer> wordcount : wordcount_text_list) {
                wordcounts_maps.add(new HashMap<>(wordcount.collectAsMap()));
            }
            List<List<String>> wordcount_list = sc.parallelize(wordcounts_maps).map(hmap -> convert_map(hmap)).collect();

            JavaRDD<Integer> indexes = sc.parallelize(IntStream.range(0, wordcount_list.size()).boxed().collect(Collectors.toList()));
            List<RowWikipedia> tmp = indexes.map(index -> setRow(index, titles, k, links_f, cat_f, wordcount_list)).collect();

            String output = args[1];

            if (tmp.size() > 0) {
                Dataset<Row> dataFrame = spark.createDataFrame(tmp, RowWikipedia.class);
                HashSet<String> k_unique = new HashSet<>();
                k_unique.addAll(k);
                HashSet<String> title_unique = new HashSet<>();
                title_unique.addAll(titles);


                for (String y : k_unique) {
                    for (String t : title_unique) {
                        Dataset<Row> year_df = dataFrame.filter(col("year").$eq$eq$eq(y)).filter(col("title").$eq$eq$eq(t));
                        year_df.show();
                        if (year_df.count() > 0) {
                            String[] t_splitted = t.split(":");
                            if (t_splitted.length < 2) {
                                Path path_output = new Path(output + "/" + y + "/" + t.replace(" ", "_"));
                                if (!fs.exists(path_output)) {
                                    year_df.write().format("json").save(path_output.toString());
                                }
                            }
                        }
                    }
                }
            }

            sc.close();
            spark.close();

        }


    }
}







