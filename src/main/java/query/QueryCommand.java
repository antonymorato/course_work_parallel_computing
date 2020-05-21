package query;


import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;


public class QueryCommand {


    private Map<String,List<String>> dictionary;


    public Map<String, List<String>> getDictionary() {
        return dictionary;
    }


    public void setDictionary(Map<String, List<String>> dictionary) {
        this.dictionary = dictionary;
    }


    public List<String> performKeywordQuery(String query){
        List<String> results = null;

        results = this.dictionary.get(query.toLowerCase());

        //removing duplicates
        if(results!=null) {
            List<String> noDuplicatesResults = new ArrayList<>(new LinkedHashSet<>(results));
            return noDuplicatesResults;
        }else return results;


    }






}
