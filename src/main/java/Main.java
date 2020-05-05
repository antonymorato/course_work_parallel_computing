import indexing.SPIMI;
import token.DocIndex;
import token.Tokenizer;
import util.FileUtil;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class Main {
    public static void main(String[] args) {


        try {
            List<DocIndex> documents=tokenizeAllDocuments();
            for (DocIndex d:documents
                 ) {
                System.out.println(d);
            }
            runSPIMIalgorithm(documents);

        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    public static List<DocIndex> tokenizeAllDocuments() throws InterruptedException {
        File [] files= FileUtil.getFiles();
        for (File f: files
             ) {
            System.out.println("somePath:"+f.getAbsolutePath());

        }
        final int NUMBER_OF_FILES=files.length;
        System.out.println("numFiles:"+NUMBER_OF_FILES);
        List<DocIndex> allDocuments = new ArrayList<>();

        // manage the pool of threads and start the SPIMI when we're done.
        ExecutorService es = Executors.newCachedThreadPool();
        String path="E:\\3 kurs\\2_semestr\\PO\\spimi-implementation-p1\\resources";
        for (int i = 0; i < NUMBER_OF_FILES; i++) {
            final int iterator = i;
            Runnable task = () -> {
                String threadName = Thread.currentThread().getName();
                System.out.println("Hello " + threadName);

                Tokenizer tokenizer = new Tokenizer(files[iterator].getAbsolutePath());
                        //"7_" + String.format("%1d", iterator+1) + ".txt");
                tokenizer.readDocuments();

                allDocuments.addAll(tokenizer.getDocumentList());

            };
            es.execute(task);
        }
        es.shutdown();

        @SuppressWarnings("unused")
        boolean finished = es.awaitTermination(1, TimeUnit.MINUTES);

        return allDocuments;
    }
    public static void runSPIMIalgorithm(List<DocIndex> documents) throws IOException {

        SPIMI spimi = new SPIMI(650000, 650000);

        Iterator<DocIndex> documentStream = documents.iterator();
        spimi.setDocIndexStream(documentStream);

        while (documentStream.hasNext()) {
            spimi.SPIMIInvert();
        }

        // This will write the dictionary to disk.
        spimi.mergeAllBlocks();

    };
}
