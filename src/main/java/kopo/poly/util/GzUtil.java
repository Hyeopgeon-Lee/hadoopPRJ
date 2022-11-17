package kopo.poly.util;

import lombok.extern.slf4j.Slf4j;

import java.io.*;
import java.util.zip.GZIPInputStream;
import java.util.zip.ZipException;

@Slf4j
public class GzUtil {

    static void gunzip(String fileName) throws Error, IOException {

        // GZ로 압축된 파일명
        File infile = new File(fileName);

        // GZ 압축이 해제될 파일명(뒤의 확장자 gz만 제거함)
        // 예 : access_log.gz => access_log
        File outfile;

        if (fileName.endsWith(".gz")) {
            outfile = new File(fileName.substring(0, fileName.length() - 3));

        } else if (fileName.endsWith(".z")) {

            outfile = new File(fileName.substring(0, fileName.length() - 2));

        } else {
            throw new Error("압축 화일 이름이 .gz나 .z로 끝나지 않았음");
        }

        if (outfile.exists())
            throw new Error("화일 " + outfile + "이 이미 존재함");

        try {

            // 파일을 스트림 형태로 변환
            FileInputStream fis = new FileInputStream(infile);

            // Gz 파일로 압축된 파일스트림을 압축해제한 스트림으로 변경
            GZIPInputStream gzis = new GZIPInputStream(fis);

//            // 압축해제된 파일을 생성하기 위해 파일 만들기
//            FileOutputStream fos = new FileOutputStream(outfile);
//
//            byte buf[] = new byte[1024];
//
//            for (int cnt; (cnt = gzis.read(buf)) != -1; ){
//                fos.write(buf, 0, cnt);
//            }
//
//
//
//            fos.close();
//
//            gzis.close();
//
//            infile.delete();


            InputStreamReader streamReader = new InputStreamReader(gzis);

            try (BufferedReader lineReader = new BufferedReader(streamReader)) {
                StringBuilder responseBody = new StringBuilder();

                String line;
                while ((line = lineReader.readLine()) != null) {
                    responseBody.append(line);
                }

//                return responseBody.toString();
            } catch (IOException e) {
                throw new RuntimeException("API 응답을 읽는데 실패했습니다.", e);
            }


        } catch (ZipException ex) {
            throw new Error("ZIP 화일 " + infile + "이 손상되었음");

        }

    }


}
