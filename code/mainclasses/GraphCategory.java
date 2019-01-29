package mainclasses;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.*;
import org.apache.spark.sql.api.java.UDF4;
import org.apache.spark.sql.api.java.UDF5;
import org.apache.spark.sql.api.java.UDF6;
import org.apache.spark.sql.types.DataTypes;
import org.neo4j.driver.v1.*;
import scala.collection.Seq;
import scala.math.BigInt;

import java.io.IOException;
import java.net.URI;
import java.util.*;

import static org.apache.spark.sql.functions.*;

public class GraphCategory implements AutoCloseable {
    private final Driver driver;

    public GraphCategory(String uri, String username, String password) {
        //creation of the driver to connect to the db
        driver = GraphDatabase.driver(uri, AuthTokens.basic(username, password));

    }

    //distruction of the driver
    @Override
    public void close() throws Exception {
        driver.close();
    }

    public void createNodes(String page, String title, String year, String category, String category_label) {
        String pageToCreate = page + category_label + year;
        String label = "_" + category_label + year;
        System.out.println("---- creation node -----" + title);
        try (Session session = driver.session();) {
            String creation = session.writeTransaction(new TransactionWork<String>() {
                @Override
                public String execute(Transaction tx) {
                    StatementResult result = tx.run("CREATE (" + pageToCreate + ":" + label + " {title:'" + title + "', year:'" + year + "', category:'" + category + "'}) RETURN " + pageToCreate + ".title");
                    return result.single().get(0).toString();
                }
            });
        }
    }

    public void createLinks() {

        try (Session session = driver.session();) {
            String creation = session.writeTransaction(new TransactionWork<String>() {
                @Override
                public String execute(Transaction tx) {
                    //System.out.println(page + " ------> " + page_link);
                    StatementResult result = tx.run("match (p), (l) where p.category = l.category and p.year=l.year and p<>l merge (p)-[link_to:LINK_TO {category: l.category, year: l.year}]-(l) return count(p)");
                    System.out.println("Crato link per "+result.toString()+" nodi");

                    return result.toString();
                }
            });
        }

    }


    public static void creationPrincipalNodes(String file, SQLContext sqlContext1) throws IOException, InterruptedException, Exception {
        Dataset<Row> dt = sqlContext1.read().json(file);

        //dataset with columns title, Complete_dataset(username, year, timestamp, text, links, references, wordcount)
        Dataset<Row> completeConc = dt.select(col("title"), functions.callUDF("concatItems", col("year"), col("links"), col("wordcount"), col("categories")).alias("Complete_dataset"));
        Dataset<Row> to_split = completeConc.select(col("title"), explode(col("Complete_dataset")).alias("Splitted"));
        Dataset<Row> final_df = to_split.selectExpr("title", "split(Splitted, '&&&')[0] as year", "split(Splitted, '&&&')[1] as links", "split(Splitted, '&&&')[2] as wordcount", "split(Splitted, '&&&')[3] as categories")
                .selectExpr("title", "year", "links", "wordcount", "categories");

        final_df.show();
        Row last_row = (final_df.collectAsList()).get(final_df.collectAsList().size() - 1);

        try (GraphCategory greeter = new GraphCategory("bolt://localhost:7687", "neo4j", "serverneo4j")) {

            String column_categories = last_row.getString(4);
            String[] categories_actualNode = column_categories.split("\\$\\$");
            String year = last_row.getString(1);
            String title = last_row.getString(0);
            for (String cat : categories_actualNode) {
                String new_title = "_" + title;
                String page = new_title.replace(" ", "_");
                String category_label = cat;
                String[] char_to_change = {" ", "(", ")", "-", "–", ".", "'", "/", "*", "’", ",", "&", "+", "!", "#", "@", "$", ";", "%", "?", "=", "<", ">", "\\", "^", "[", "]", "{", "}", "\""};

                for (String c : char_to_change) {
                    if (c.equals("+")) {
                        page = page.replace(c, "plus_");
                        category_label = category_label.replace(c, "plus_");
                        title=title.replace(c, "plus");
                        cat=cat.replace(c, "plus");
                    } else if (c.equals("@")) {
                        page = page.replace(c, "at");
                        category_label = category_label.replace(c, "at");
                        title=title.replace(c, "at");
                        cat=cat.replace(c, "at");
                    } else if (c.equals("-")) {
                        page = page.replace(c, "__");
                        category_label = category_label.replace(c, "__");
                        title=title.replace(c, "__");
                        cat=cat.replace(c, "__");
                    } else if (c.equals("=")) {
                        title = title.replace(c, "equal_");
                        category_label = category_label.replace(c, "equal_");
                        title = title.replace(c, "equal_");
                        cat=cat.replace(c, "equal_");
                    } else {
                        page = page.replace(c, "_");
                        category_label = category_label.replace(c, "_");
                        title = title.replace(c, "_");
                        cat=cat.replace(c, "_");
                    }
                }

                System.out.println("Nodo: "+title+"   "+year+"  "+cat);
                greeter.createNodes(page, title, year, cat, category_label);
            }


        }
    }


    public static void createLinksMain() throws IOException, InterruptedException, Exception {

        try (GraphCategory greeter = new GraphCategory("bolt://localhost:7687", "neo4j", "serverneo4j")) {


            greeter.createLinks();


        }
    }


    public static void main(String[] args) throws IOException, InterruptedException, Exception {
        SparkSession spark1 = SparkSession.builder().appName("try Spark").master("local").getOrCreate();
        JavaSparkContext sc1 = new JavaSparkContext(spark1.sparkContext());
        SQLContext sqlContext1 = new SQLContext(sc1);

        //@args[0] = path output directories
        //read all the "years" directories path
        String path = args[0];
        System.out.println("-------  read path " + path + "----------------");
        List<String> subpaths = new ArrayList<>();
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(new URI(path), conf);
        FileStatus[] fileStatus = fs.listStatus(new Path(path));
        for (FileStatus status : fileStatus) {
            subpaths.add(status.getPath().toString());
            System.out.println("Subpath  " + status.getPath().toString());
        }
        fs.close();

        System.out.println("-------  read all subpaths  SIZE " + subpaths.size() + " ----------------");


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
        List<String> subpathLev2 = new ArrayList<>();
        for (int i = 1; i <= number_of_dir; i++) {
            System.out.println("---------------------  on the loop   -----------------------------");
            HashSet<String> categories_available = new HashSet<>();
            String singleYear = subpaths.get(number_of_dir - i);
            System.out.println("single year " + singleYear);
            String[] tokens = singleYear.split("/");
            String actual_year = tokens[7];
            System.out.println("------------------     Actual year:" + actual_year + "-------------------------------------");

            //read all the "page" directories path
            List<String> subpathsLev1 = new ArrayList<>();
            Configuration confLev1 = new Configuration();
            FileSystem fsLev1 = FileSystem.get(new URI(singleYear), conf);
            FileStatus[] fileStatusLev1 = fsLev1.listStatus(new Path(singleYear));
            for (FileStatus status : fileStatusLev1) {
                subpathsLev1.add(status.getPath().toString());
            }
            fsLev1.close();
            System.out.println("------------- Subpaths lev1 " + subpathsLev1.size() + "--------------");
            //read all the "json" files
            List<String> subpathLev2tmp = new ArrayList<>();
            for (String elem : subpathsLev1) {
                Configuration confLev2 = new Configuration();
                FileSystem fsLev2 = FileSystem.get(new URI(elem), conf);
                FileStatus[] fileStatusLev2 = fsLev2.listStatus(new Path(elem));
                for (FileStatus status : fileStatusLev2) {
                    subpathLev2tmp.add(status.getPath().toString());
                }

                fsLev2.close();
            }


            for (String elem : subpathLev2tmp) {
                String[] tokens2 = elem.split("/");
                if (!tokens2[9].equals("_SUCCESS")) {
                    subpathLev2.add(elem);
                }
            }


        }
        System.out.println("Number of path "+subpathLev2.size());
        for (String file: subpathLev2){
            System.out.println(file);
        }



        for (String file : subpathLev2) {
            System.out.println("==================    Creation principal nodes from file " + file + "    ================");
            creationPrincipalNodes(file, sqlContext1);
        }
        System.out.println("Sto per creare tutti i link");
        createLinksMain();


    }
}