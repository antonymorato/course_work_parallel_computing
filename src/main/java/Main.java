import com.google.common.collect.Lists;
import indexing.SPIMI;
import token.DocIndex;
import token.Tokenizer;
import util.FileUtil;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Scanner;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class Main {
    private static int THREADS=5;
    private static Scanner scanner=new Scanner(System.in);

    public static void main(String[] args) {


        try {

            List<DocIndex> documents=tokenizeAllDocuments();
            runSPIMIalgorithm(documents);


        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    public static void menu(){

        for (;;){
            short choice;
            System.out.println("MENU");
            System.out.println("1.Change number of threads.");
            System.out.println("2.Run SPIMI");
            System.out.println("3.EXIT");
            choice=getChoice();

            switch (choice){
                case 3:System.exit(1);
                default:
                    System.out.println("Wrong enter, repeat please");
            }

        }



    }

    public static String getPath(){
        System.out.println("Enter path to \"acl\" directory:");

        return scanner.nextLine();
    }
    public static short getChoice(){
        System.out.println("Enter your choice:");
        return scanner.nextShort();
    }


    public static List<DocIndex> tokenizeAllDocuments() throws InterruptedException {

        File [] files= FileUtil.getFiles();

        final int NUMBER_OF_FILES=files.length;
        List<DocIndex> allDocuments = new ArrayList<>();

        // manage the pool of threads and start the SPIMI when we're done.
        ExecutorService es = Executors.newCachedThreadPool();

        String path="E:\\3 kurs\\2_semestr\\PO\\spimi-implementation-p1\\resources";
        for (int i = 0; i < NUMBER_OF_FILES; i++) {
            final int iterator = i;
            Runnable task = () -> {
                final int NUM=NUMBER_OF_FILES/THREADS;


                    Tokenizer tokenizer = new Tokenizer(files[iterator].getAbsolutePath());
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
    public static void runSPIMIalgorithm(List<DocIndex> documents) throws IOException, InterruptedException {

//        Spliterator<DocIndex> spliterator= documents.spliterator();
//        spliterator.

        //SPIMI spimi = new SPIMI(650000, 650000);


//        Iterator<DocIndex> documentStream = documents.iterator();
//        spimi.setDocIndexStream(documentStream);
//
//        while (documentStream.hasNext()) {
//            spimi.SPIMIInvert();
//        }
//
//
////         This will write the dictionary to disk.
//        spimi.mergeAllBlocks();
        ExecutorService es = Executors.newCachedThreadPool();

        long end=System.nanoTime();
        for (int i = 0; i <THREADS ; i++) {
                final int iterator=i;

            Runnable task = () -> {
                SPIMI spimi = new SPIMI(650000, 650000);
                List<List<DocIndex>> sublists = Lists.partition(documents, THREADS);
                Iterator<DocIndex> documentStream = sublists.get(iterator).iterator();
                spimi.setDocIndexStream(documentStream);

                while (documentStream.hasNext()) {
                        spimi.SPIMIInvert();
                     }
            };
            es.execute(task);
        }
        es.shutdown();
        @SuppressWarnings("unused")
        boolean finished = es.awaitTermination(1, TimeUnit.MINUTES);

    }
}
