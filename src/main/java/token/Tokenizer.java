package token;

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.Setter;

import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;


@Getter
@Setter
@RequiredArgsConstructor
public class Tokenizer {

    private String fileName;
    private String fileContents;

    private List<DocIndex> documentList;



    private String readFile(Charset encoding) throws IOException, URISyntaxException {
        ClassLoader classloader = Thread.currentThread().getContextClassLoader();
        URL url = classloader.getResource(this.fileName);
        byte[] encoded = Files.readAllBytes(Paths.get(url.toURI()));
        return new String(encoded, encoding);
    }
    /**
     * Reads the files with reuters news    
     */
    public void readDocuments() {
        String fileContents = "";
        try {
            fileContents = this.readFile(Charset.forName("utf-8"));
            this.fileContents = fileContents;
            this.tokenizeDocument();
            //TODO add logger
        } catch (IOException e) {
            e.printStackTrace();
        } catch (URISyntaxException use) {
            use.printStackTrace();
        }

    }

    /**
     * tokenizes the document
     */
    private void tokenizeDocument() {
        this.removeStringGarbage();
        this.splitFileContents();
    }
    private void removeStringGarbage() {
        this.fileContents = this.fileContents.replaceAll("(?:&#[0-9]*;)", " ");
        this.fileContents = this.fileContents.replaceAll("\\,*", "");
        this.fileContents = this.fileContents.replaceAll("\\.*", "");
    };
    private void splitFileContents() {
        // Consists of docId and list of all documents
        this.documentList = new ArrayList<DocIndex>();


        List<String> news = new ArrayList<String>();

        news.addAll(Arrays.asList(this.fileContents.split("\\.\\s+")));



        //For each word in the news.
        for (String tokens : news) {
            String docID = parseDocumentID(tokens.split("\n")[0]);
           // tokens = Jsoup.clean(tokens, Whitelist.none());
            tokens = cleanToken(tokens);


            tokens = removeNumbers(tokens);
            tokens = caseFolding(tokens);


            //Splits to get the terms.
            String[] terms = tokens.split("\\s");

            //filling up the documentlist.
            DocIndex docIndex = new DocIndex(docID, terms);
            this.documentList.add(docIndex);
        }


    }

    private static String cleanToken(String tokens){
        tokens = tokens.replaceAll("\n|\r", " ");
        tokens = tokens.replaceAll("\"", "");
        tokens = tokens.replaceAll("&lt;", "");
        tokens = tokens.replaceAll("&gt;", "");
        tokens = tokens.replaceAll("\\+", "");
        tokens = tokens.replaceAll("\\(|\\)", "");
        tokens = tokens.replaceAll("\\*", "");
        tokens = tokens.replaceAll("'", "");
        tokens = tokens.replaceAll("&amp;", "");
        tokens = tokens.replaceAll("-", "");
        return tokens;
    }

    private static String removeNumbers(String tokens){
        tokens = tokens.replaceAll("[0-9]+", "");
        return tokens;
    }

    private static String caseFolding(String tokens){
        tokens = tokens.toLowerCase();
        return tokens;
    }


    private static String parseDocumentID(String fileName) {
        String res=fileName.replaceAll("[^0-9]","");

        return res;
    }
}
