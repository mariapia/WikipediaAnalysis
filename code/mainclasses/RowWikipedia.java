package mainclasses;
import org.apache.spark.api.java.JavaPairRDD;

import java.io.Serializable;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

public class RowWikipedia implements Serializable {
    private String Title;
    private String Year;
    private HashSet<String> Links;
    //private HashMap<String, Integer> Wordcount;
    private List<String> Wordcount;


    private List<String> Categories;


    public List<String> getCategories() {
        return Categories;
    }

    public void setCategories(List<String> categories) {
        Categories = categories;
    }


    public String getTitle() {
        return Title;
    }

    public List<String> getWordcount() {
        return Wordcount;
    }

    public void setWordcount(List<String> wordcount) {
        Wordcount = wordcount;
    }

    public void setTitle(String title) {
        Title = title;
    }

    public String getYear() {
        return Year;
    }

    public void setYear(String year) {
        Year = year;
    }


    public HashSet<String> getLinks() {
        return Links;
    }

    public void setLinks(HashSet<String> links) {
        Links = links;
    }

}