import token.DocIndex;
import token.Tokenizer;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class Main {
    private static int NUMBER_OF_THREADS=1;
    public static void main(String[] args) {


        try {
            List<DocIndex> docIndices=tokenizeAllDocuments();
            for (DocIndex d:docIndices
                 ) {
                System.out.println(d);
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

    }

    public static List<DocIndex> tokenizeAllDocuments() throws InterruptedException {
        List<DocIndex> allDocuments = new ArrayList<DocIndex>();

        // manage the pool of threads and start the SPIMI when we're done.
        ExecutorService es = Executors.newCachedThreadPool();
        String path="E:\\3 kurs\\2_semestr\\PO\\spimi-implementation-p1\\resources";
        for (int i = 0; i < NUMBER_OF_THREADS; i++) {
            final int iterator = i;
            Runnable task = () -> {
                String threadName = Thread.currentThread().getName();
                System.out.println("Hello " + threadName);

                Tokenizer tokenizer = new Tokenizer("7_" + String.format("%1d", iterator+1) + ".txt");
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
}
