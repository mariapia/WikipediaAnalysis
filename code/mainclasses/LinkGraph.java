package mainclasses;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.*;
import org.apache.spark.sql.api.java.UDF4;
import org.apache.spark.sql.api.java.UDF5;
import org.apache.spark.sql.api.java.UDF7;
import org.apache.spark.sql.types.DataTypes;
import org.neo4j.driver.v1.*;
import scala.collection.Seq;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.apache.spark.sql.functions.*;

public class LinkGraph implements AutoCloseable {
    private final Driver driver;

    public LinkGraph(String uri, String username, String password) {
        //creation of the driver to connect to the db
        driver = GraphDatabase.driver(uri, AuthTokens.basic(username, password));
    }
    //distruction of the driver
    @Override
    public void close() throws Exception {
        driver.close();
    }

    public void createNodes(String page, String title,  String year,  String actual_year) {
        String pageToCreate = page+actual_year;
        System.out.println("---- creation node -----"+title);
        try (Session session = driver.session();) {
            String creation = session.writeTransaction(new TransactionWork<String>() {
                @Override
                public String execute(Transaction tx) {
                    StatementResult result = tx.run("CREATE (" + pageToCreate + ":Page"+actual_year+" {title:'" + title + "', year:'" + year + "'}) RETURN " + page+actual_year + ".title");
                    return result.single().get(0).toString();
                }
            });
        }
    }

    public void createLinks(String page, String title, String link, String page_link, String actual_year, Integer weight) {
        System.out.println("--- PARAMETERS. Page from "+page+", title from "+title+", page to "+page_link+", title to "+link);

        try (Session session = driver.session();) {
            String creation = session.writeTransaction(new TransactionWork<String>() {
                @Override
                public String execute(Transaction tx) {
                    String pageToCreate = page_link+actual_year;
                    String nameRelation = page+"_TO_"+pageToCreate;
                    System.out.println("--page to create -- "+pageToCreate);
                    //System.out.println(page + " ------> " + page_link);
                    StatementResult result = tx.run("MERGE  (" + pageToCreate + ":Page"+actual_year+" { title:'" + link + "', year: '"+actual_year+"'}) " +
                            "WITH " + pageToCreate + ".title AS titleLink " +
                            "MATCH  (p:Page"+actual_year+" {title: '" + title + "'}), (l:Page"+actual_year+" {title: '" + link + "', year: '"+actual_year+"'}) " +
                            //"(l {title: titleLink, year: '"+actual_year+"'}) " +
                            "MERGE  (p)-[link_to:LINK_TO_"+actual_year+" {weight:'" + weight + "', name:'" + nameRelation + "', from: '"+title+"', to: '"+link+"'}]->(l) " +
                            "RETURN link_to.name ");

                    return result.single().get(0).toString();
                }
            });
        }
    }


    public static void creationPrincipalNodes(String file, SQLContext sqlContext1, String actual_year) throws IOException, InterruptedException, Exception {
        Dataset<Row> dt = sqlContext1.read().json(file);
        //System.out.println("In path " + file);

        //dataset with columns title, Complete_dataset(username, year, timestamp, text, links, references, wordcount)
        Dataset<Row> completeConc = dt.select(col("title"), functions.callUDF("concatItems", col("year"),  col("links"), col("wordcount"), col("categories")).alias("Complete_dataset"));
//        //dataset with one couple (username, timestamp) per line

        Dataset<Row> to_split = completeConc.select(col("title"), explode(col("Complete_dataset")).alias("Splitted"));
//        System.out.println("------sto per creare il dataset finale");

        Dataset<Row> final_df = to_split.selectExpr("title", "split(Splitted, '&&&')[0] as year", "split(Splitted, '&&&')[1] as links", "split(Splitted, '&&&')[2] as wordcount", "split(Splitted, '&&&')[3] as categories")
                .selectExpr("title", "year", "links", "wordcount", "categories");


        try (LinkGraph greeter = new LinkGraph("bolt://localhost:7687", "neo4j", "serverneo4j")) {
            Row last_row = (final_df.collectAsList()).get(final_df.collectAsList().size() - 1);
            String title = last_row.getString(0);
            String new_title = "_"+title;
            String page = new_title.replace(" ", "_");

            String [] char_to_change = {" ","(",")","-","–",".","'","/","*","’",",","&","+"};

            for (String c : char_to_change){
                if (c.equals("+")){
                    page = page.replace(c,"plus_");
                } else {
                    page = page.replace(c, "_");
                }
            }

            String year = last_row.getString(1);
            greeter.createNodes(page, title,  year,  actual_year);

        }
    }

    public static void creationLinksAndPages(String file, String output_path, SQLContext sqlContext1, String actual_year) throws IOException, InterruptedException, Exception {
        Dataset<Row> dt = sqlContext1.read().json(file);
        System.out.println("In path " + file);

        FileSystem fs = FileSystem.get(new URI(output_path), sqlContext1.sparkContext().hadoopConfiguration());

        //dataset with columns title, Complete_dataset(username, year, timestamp, text, links, references, wordcount)
        Dataset<Row> completeConc = dt.select(col("title"), functions.callUDF("concatItems", col("year"), col("links"),  col("wordcount"), col("categories")).alias("Complete_dataset"));

        //dataset with one couple (username, timestamp) per line
        Dataset<Row> to_split = completeConc.select(col("title"), explode(col("Complete_dataset")).alias("Splitted"));

        Dataset<Row> final_df = to_split.selectExpr("title", "split(Splitted, '&&&')[0] as year", "split(Splitted, '&&&')[1] as links",  "split(Splitted, '&&&')[2] as wordcount",  "split(Splitted, '&&&')[3] as categories")
                .selectExpr("title", "year","links","wordcount", "categories");
        List<String> links_array = new ArrayList<>();
        List<String> page_links_array = new ArrayList<>();

        List<String> dest_links_array = new ArrayList<>();
        try (LinkGraph greeter = new LinkGraph("bolt://localhost:7687", "neo4j", "serverneo4j")) {
            Row last_row = (final_df.collectAsList()).get(final_df.collectAsList().size() - 1);
            String title = last_row.getString(0);
            String page = title.replace(" ", "_");
            String year = last_row.getString(1);
            String links = last_row.getString(2);

            String[] tokens = links.split("\\$\\$");


            for (String token : tokens) {
                if (!token.equals("")) {
                    String link_path = output_path + year + "/" + token.toLowerCase().replace(" ", "_") + "/";

                    String new_token = "_" + token;
                    String [] char_to_change = {" ","(",")","-","–",".","'","/","*","’",",","&","+"};
                    String page_link = new_token.replace(" ", "_");

                    for (String c : char_to_change){
                        if (c.equals("+")){
                            page_link = page_link.replace(c,"plus_");
                        } else {
                            page_link = page_link.replace(c, "_");
                        }
                    }

                    System.out.println(" ^^^^^^^^^^^^^^ link: "+page_link+"     FILE : " + link_path);

                    if (fs.isDirectory(new Path(link_path))) {

                        Dataset<Row> dest_link_df = sqlContext1.jsonFile(link_path).select(concat_ws(",", col("links")));
                        System.out.println("oooooooooooooooo   DEST LINK DF      ooooooooooo");
                        dest_link_df.show();
                        String last_link = dest_link_df.collectAsList().get(dest_link_df.collectAsList().size() - 1).toString();
                        System.out.println("LAST LINK " + last_link);

                        links_array.add(token);
                        page_links_array.add(page_link);
                        dest_links_array.add(last_link);
                    } else {
                        greeter.createLinks(page, title, token.replace("'","_"), page_link, actual_year, 0);
                    }
                }
            }

            for (int i = 0; i < links_array.size(); i++) {
                String link = links_array.get(i);
                String page_link = page_links_array.get(i);

                String [] dest_link_formatted = dest_links_array.get(i).split(",");

                List<String> links_dest = new ArrayList<>();
                for (String l : dest_link_formatted){
                    System.out.println("DEST_LINK " + l);
                    links_dest.add(l);
                }

                for(String s : tokens){
                    System.out.println("PAGE_LINK " + s);

                }

                Integer weight = computeLinkWeight(tokens, dest_link_formatted);
                greeter.createLinks(page, title, link, page_link, actual_year, weight);
            }

        }
    }

    private static Integer computeLinkWeight(String[] page_link, String[] dest_link_formatted) {
        Integer weight = 0;
        for (String l : page_link){
            if (Arrays.asList(dest_link_formatted).contains(l)){
                weight += 1;
            }
        }
        return weight;
    }


    public static void main(String[] args) throws IOException, InterruptedException, Exception {
        SparkSession spark1 = SparkSession.builder().appName("try Spark").master("local").getOrCreate();
        JavaSparkContext sc1 = new JavaSparkContext(spark1.sparkContext());
        SQLContext sqlContext1 = new SQLContext(sc1);

        //@args[0] = path output directories
        //read all the "years" directories path
        String path = args[0];
        List<String> subpaths = new ArrayList<>();
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(new URI(path), conf);
        FileStatus[] fileStatus = fs.listStatus(new Path(path));
        for (FileStatus status : fileStatus) {
            subpaths.add(status.getPath().toString());
        }


        UDF4 concatItems = new UDF4<String, Seq<String>, Seq<String>, Seq<String>, ArrayList<String>>() {

            public ArrayList<String> call(final String col1, final Seq<String> col2, final Seq<String> col3, final Seq<String> col4) throws Exception {

                ArrayList zipped = new ArrayList();
                String subRow = col1;


                //concat all elements of col4 (links)
                StringBuilder subCol2 = new StringBuilder();
                for (int i = 0, listSize = col2.size(); i < listSize; i++) {
                    if (i != col2.size() - 1) {
                        subCol2.append(col2.apply(i) + "$$");
                    } else {
                        subCol2.append(col2.apply(i));
                    }
                }

                //concat all elements of col5 (wordcount)
                StringBuilder subCol3 = new StringBuilder();
                for (int i = 0, listSize = col3.size(); i < listSize; i++) {
                    if (i != col3.size() - 1) {
                        subCol3.append(col3.apply(i) + "$$");
                    } else {
                        subCol3.append(col3.apply(i));
                    }
                }

                //concat all elements of col6 (categories)
                StringBuilder subCol4 = new StringBuilder();
                for (int i = 0, listSize = col4.size(); i < listSize; i++) {
                    if (i != col4.size() - 1) {
                        subCol4.append(col4.apply(i) + "$$");
                    } else {
                        subCol4.append(col4.apply(i));
                    }
                }


                subRow = subRow + "&&&" + subCol2.toString() + "&&&" + subCol3.toString() + "&&&" + subCol4.toString();
                //System.out.println("\n------------------- subCol8 "+subCol8.toString()+"------------------------------\n");
                zipped.add(subRow);
                return zipped;
            }
        };
        //Function registration
        spark1.udf().register("concatItems", concatItems, DataTypes.createArrayType(DataTypes.StringType));

        int number_of_dir = subpaths.size();
        for (int i =1 ; i<=number_of_dir; i++){
            String singleYear = subpaths.get(number_of_dir-i);
            String[] tokens = singleYear.split("/");
            String actual_year = tokens[6];
            System.out.println(actual_year);

            //read all the "page" directories path
            List<String> subpathsLev1 = new ArrayList<>();
            Configuration confLev1 = new Configuration();
            FileSystem fsLev1 = FileSystem.get(new URI(singleYear), conf);
            FileStatus[] fileStatusLev1 = fsLev1.listStatus(new Path(singleYear));
            for (FileStatus status : fileStatusLev1) {
                subpathsLev1.add(status.getPath().toString());
            }

            //read all the "json" files
            List<String> subpathLev2tmp = new ArrayList<>();
            for (String elem : subpathsLev1) {
                Configuration confLev2 = new Configuration();
                FileSystem fsLev2 = FileSystem.get(new URI(elem), conf);
                FileStatus[] fileStatusLev2 = fsLev2.listStatus(new Path(elem));
                for (FileStatus status : fileStatusLev2) {
                    subpathLev2tmp.add(status.getPath().toString());
                }
            }

            List<String> subpathLev2 = new ArrayList<>();
            for (String elem : subpathLev2tmp) {
                String[] tokens2 = elem.split("/");
                if (!tokens2[8].equals("_SUCCESS")) {
                    subpathLev2.add(elem);
                }
            }

            for (String file : subpathLev2) {

                creationPrincipalNodes(file, sqlContext1, actual_year);
            }

            for (String file : subpathLev2) {
                creationLinksAndPages(file, args[0], sqlContext1, actual_year);
            }
        }
    }
}