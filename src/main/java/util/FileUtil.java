package util;

import lombok.Setter;

import java.io.File;

import static util.GlobalConst.*;

public class FileUtil {

    private static String aclPath="E:\\3_kurs\\2_semestr\\PO\\aclImdb";

    public static String getAclPath(){return aclPath;}

    public static void setAclPath(String aclPath) {
        FileUtil.aclPath = aclPath;
    }

    public static File[] getFiles() {
        int startIdx1 = FILES_TOTAL1/PARTS_NUMBER * (VARIANT - 1);
        int startIdx2 = FILES_TOTAL2/PARTS_NUMBER * (VARIANT - 1);

        String dir1 = aclPath+"\\test\\neg";
        String dir2 = aclPath+"\\test\\pos";
        String dir3 = aclPath+"\\train\\neg";
        String dir4 = aclPath+"\\train\\pos";
        String dir5 = aclPath+"\\train\\unsup";

        FileSearch fileSearch = new FileSearch();
        fileSearch.addFilesFromDir(dir1, startIdx1, FILES_NEED1);
        fileSearch.addFilesFromDir(dir2, startIdx1, FILES_NEED1);
        fileSearch.addFilesFromDir(dir3, startIdx1, FILES_NEED1);
        fileSearch.addFilesFromDir(dir4, startIdx1, FILES_NEED1);
        fileSearch.addFilesFromDir(dir5, startIdx2, FILES_NEED2);

        return fileSearch.getFilesAbsolutePath();
    }
}
