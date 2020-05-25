package util;

import org.apache.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static util.GlobalConst.aclPath;

public class FileReader {
    private static String testNeg="\\test\\neg";
    private static String testPos="\\test\\pos";
    private static String trainNeg="\\train\\neg";
    private static String trainPos="\\train\\pos";
    private static String trainUnsup="\\train\\unsup";
    private static Logger logger;
    {
        logger=Logger.getLogger(FileReader.class);
    }


    private static Set<String> countFilePrefix(int filesCount){
        int startIndex=9250;
        int endIndex=startIndex+filesCount;
        Set<String> filePrefixes=new HashSet<>();

        for (int i=startIndex;i<endIndex;i++)
        {
            filePrefixes.add(String.valueOf(i));
        }
        return filePrefixes;
    }
    private static List<File> collectOneDir(String subPath, int filesNeeded)
    {
        final Set<String> prefixes=countFilePrefix(filesNeeded);
        List<File> files = null;
        try {
            files=Files.walk(Paths.get(aclPath+subPath))
                    .filter(n->{
                        for (String s:prefixes) {
                            if (n.getFileName().toString().startsWith(s)) {
                                return true;
                            }
                        }
                        return false;
                    })
                    .map(Path::toFile)
                    .collect(Collectors.toList());


        } catch (IOException e) {
            logger.error(e);
        }

        return files;
    }


    public static List<File> collectAll(){
        List<File> allFiles=new ArrayList<>();

        allFiles.addAll(collectOneDir(testNeg,250));
        allFiles.addAll(collectOneDir(testPos,250));
        allFiles.addAll(collectOneDir(trainNeg,250));
        allFiles.addAll(collectOneDir(trainPos,250));
        allFiles.addAll(collectOneDir(trainUnsup,1000));

        return allFiles;
    }







}
