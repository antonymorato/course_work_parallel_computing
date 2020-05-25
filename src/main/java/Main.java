import indexing.SPIMI;
import org.apache.log4j.Logger;
import query.QueryCommand;
import token.DocIndex;
import token.Tokenizer;
import util.FileReader;
import util.GlobalConst;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Scanner;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static util.GlobalConst.aclPath;

public class Main {
    private static int THREADS;
    private static Scanner scanner;
    private static Logger logger;
    private static long StartSpimi;
    private static long EndSpimi;
   static
    {
        THREADS=10;
        scanner=new Scanner(System.in);
        logger=Logger.getLogger(Main.class);

    }
    public static void main(String[] args) {

        menu();
    }

    public static void menu(){

        for (;;){
            short choice;
            System.out.println("MENU");
            System.out.println("1.Change number of threads");
            System.out.println("2.Change out .txt directory");
            System.out.println("3.Change \"acllmdb\" directory");
            System.out.println("4.Run SPIMI");
            System.out.println("5.Run test queries");
            System.out.println("6.EXIT");
            choice=getChoice();

            switch (choice){
                case 1:{
                    THREADS=getThreads();
                    break;
                }
                case 2:{
                    GlobalConst.setOutFilesPath(getOutPath());
                    break;
                }
                case 3: {
//                    FileUtil.setAclPath(
                            aclPath=getAclPath();
                    break;
                }
                case 4: {
                    start();
                    break;
                }
                case 5:{
                    performTestQueries();
                    break;
                }
                case 6:{
                    System.exit(1);
                    break;
                }
                default:
                    System.out.println("Wrong enter, repeat please");
            }

        }



    }

    public static void start() {
        try {
            logger.info("New spimi.");
            logger.info("acllmdb dir:"+aclPath);

            long startTokenizer=System.nanoTime();
            List<DocIndex> documents=tokenizeAllDocuments();
            long endTokenizer=System.nanoTime();

            long startSpimi=System.nanoTime();
            runSPIMIalgorithm(documents);
            long endSpimi=System.nanoTime();

            double resTokenizer= ((endTokenizer-startTokenizer)*Math.pow(10,-9));
            double resSpimi=(EndSpimi-StartSpimi)*Math.pow(10,-9);

            logger.info("Tokenizer with "+THREADS+ " threads:"+String.format("%.4f",resTokenizer)+" sec");
            logger.info("Spimi with "+THREADS+ " threads:"+String.format("%.4f",resSpimi)+" sec");
            logger.info("Final time:"+String.format("%.4f",resSpimi+resTokenizer));
            System.out.println("Tokenizer time with "+THREADS+" thread(s):"+
                    resTokenizer);
            System.out.println("Spimi time with "+THREADS+" thread(s):"+
                    resSpimi);



        } catch (InterruptedException e) {
        //    e.printStackTrace();
        logger.error(e.getMessage());
        } catch (IOException e) {
//            e.printStackTrace();
            logger.error(e.getMessage());
        }
    }

    public static int getThreads()
    {
        System.out.println("Current number of threads:"+THREADS);
        System.out.println("Enter number of threads you wish to use(>=1):");
        int res = scanner.nextInt();
        if (res<1) {
            System.out.println("Wrong input, try again");
            res=getThreads();
        }
        return res;
    }
    public static String getOutPath(){
        System.out.println("Current path:"+ GlobalConst.outFilesPath);

        System.out.println("Enter path to out .txt directory:");

        return scanner.next();
    }
    public static String getAclPath(){
        System.out.println("Current path:"+aclPath);
        System.out.println("Enter path to \"acllmdb\" directory:");

        return scanner.next();
    }
    public static short getChoice(){
        System.out.println("Enter your choice:");
        return scanner.nextShort();
    }
    public static void performTestQueries() {

        SPIMI spimi = new SPIMI();

        QueryCommand qc = new QueryCommand();
        qc.setDictionary(spimi.readBlockAndConvertToDictionary("dictionary.txt"));

        // NULL MEANS THERE ARE NO RESULTS!

        // Uncomment this to test yourself.
        //performTestQueries(qc); //designed by myself
        //performProjectQueries(qc);
        performQueries(qc);

    }

    public static void performQueries(QueryCommand qc) {


        String testQuery = qc.performKeywordQuery("animal").toString();
        System.out.println("Keyword query 'animal' result: " + testQuery);

    }

    public static List<DocIndex> tokenizeAllDocuments() throws InterruptedException {

//        File [] files= FileUtil.getFiles();
        List<File> files= FileReader.collectAll();
        final int NUMBER_OF_FILES=files.size();
        List<DocIndex> allDocuments = new ArrayList<>();

        // manage the pool of threads and start the SPIMI when we're done.
        ExecutorService es = Executors.newCachedThreadPool();
        final int NUM = NUMBER_OF_FILES / THREADS;
        for (int i = 0; i < THREADS; i++) {
            final int iterator = i;
            Runnable task = () -> {
                for (int j = iterator; j <NUMBER_OF_FILES ; j+=THREADS) {




                    Tokenizer tokenizer = new Tokenizer(files.get(j).getAbsolutePath());
                    tokenizer.readDocuments();
                    allDocuments.addAll(tokenizer.getDocumentList());
                }
                };
            es.execute(task);
        }
        es.shutdown();

        boolean finished = es.awaitTermination(1, TimeUnit.MINUTES);


        return allDocuments;
    }

    public static void runSPIMIalgorithm(List<DocIndex> documents) throws IOException, InterruptedException {
        StartSpimi=0;
        EndSpimi=0;
        StartSpimi=System.nanoTime();

        AtomicInteger blockNumber=new AtomicInteger(0);

                SPIMI spimi = new SPIMI(650000, 650000,blockNumber);
                Iterator<DocIndex> documentStream = documents.iterator();
                spimi.setDocIndexStream(documentStream);

                while (documentStream.hasNext()) {
                        spimi.SPIMIInvert();
                     }


        spimi.mergeAllBlocks();
        EndSpimi=System.nanoTime();

    }
}
