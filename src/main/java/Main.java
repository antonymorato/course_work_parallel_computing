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
        menu();



    }

    public static void menu(){

        for (;;){
            short choice;
            System.out.println("MENU");
            System.out.println("1.Change number of threads.");
            System.out.println("2.Change \"acllmdb\" directory");
            System.out.println("3.Run SPIMI");
            System.out.println("4.EXIT");
            choice=getChoice();

            switch (choice){
                case 1:THREADS=getThreads();
                case 2:FileUtil.setAclPath(getPath());
                case 3:start();
                case 4:System.exit(1);
                default:
                    System.out.println("Wrong enter, repeat please");
            }

        }



    }

    public static void start() {
        try {
            List<DocIndex> documents=tokenizeAllDocuments();
            runSPIMIalgorithm(documents);


        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static int getThreads()
    {
        System.out.println("Enter number of threads you wish to use:");
        return scanner.nextInt();
    }
    public static String getPath(){
        System.out.println("Current path:"+FileUtil.getAclPath());
        System.out.println("Enter path to \"acl\" directory:");

        return scanner.next();
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

        SPIMI spimi = new SPIMI(650000, 650000);


        Iterator<DocIndex> documentStream = documents.iterator();
        spimi.setDocIndexStream(documentStream);

        while (documentStream.hasNext()) {
            spimi.SPIMIInvert();
        }


//         This will write the dictionary to disk.
        spimi.mergeAllBlocks();
//        ExecutorService es = Executors.newCachedThreadPool();
//        List<List<DocIndex>> sublists = Lists.partition(documents, THREADS);
//
//        final int block=1;
//        long end=System.nanoTime();
//        for (int i = 0; i <sublists.size(); i++) {
//            final int iterator=i;
//
//            Runnable task = () -> {
//                SPIMI spimi = new SPIMI(650000, 650000,block+iterator*THREADS);
//                Iterator<DocIndex> documentStream = sublists.get(iterator).iterator();
//                spimi.setDocIndexStream(documentStream);
//
//                while (documentStream.hasNext()) {
//                        spimi.SPIMIInvert();
//                     }
//            };
//            es.execute(task);
//        }
//        es.shutdown();
//        @SuppressWarnings("unused")
//        boolean finished = es.awaitTermination(1, TimeUnit.MINUTES);

    }
}
