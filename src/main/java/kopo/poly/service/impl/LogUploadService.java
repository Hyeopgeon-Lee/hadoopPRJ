package kopo.poly.service.impl;

import kopo.poly.dto.HadoopDTO;
import kopo.poly.service.IAccessLogUploadService;
import kopo.poly.service.ILogUploadService;
import kopo.poly.util.CmmUtil;
import lombok.extern.log4j.Log4j2;
import org.apache.hadoop.conf.Configuration;
import org.springframework.stereotype.Service;

import java.io.*;
import java.util.regex.Pattern;
import java.util.zip.GZIPInputStream;

@Log4j2
@Service
public class LogUploadService implements ILogUploadService {

    /*
     * 하둡 접속 설정
     * */
    private Configuration getHadoopConfiguration() {

        Configuration conf = new Configuration();

        // fs.defaultFS 설정 값 : hdfs://192.168.2.136:9000
        conf.set("fs.defaultFS", "hdfs://" + IAccessLogUploadService.namenodeHost + ":"
                + IAccessLogUploadService.namemodePort);

        // yarn 주소 설정
        conf.set("yarn.resourcemanager.address", IAccessLogUploadService.namenodeHost + ":"
                + IAccessLogUploadService.yarnPort);

        return conf;
    }

    @Override
    public String readLocalGzFileIP(HadoopDTO pDTO) throws Exception {
        log.info(this.getClass().getName() + ".readLocalGzFile Start!");

        String result = ""; // GzFile 내용 결과

        long readCnt = pDTO.getLineCnt();

        log.info("readCnt : " + readCnt);

        // 예 : c:/hadoop_data/access_log.gz
        String localFile = CmmUtil.nvl(pDTO.getLocalUploadPath()) +
                "/" + CmmUtil.nvl(pDTO.getLocalUploadFileName());

        log.info("localFile : " + localFile);

        // Gz 파일 가져오기
        File localGzFile = new File(localFile);

        // 파일 처리를 위해 파일을 스트림 형태로 변환
        FileInputStream fis = new FileInputStream(localGzFile);

        // Gz 파일로 압축된 파일스트림을 압축해제한 스트림으로 변경
        GZIPInputStream gzis = new GZIPInputStream(fis);

        // Gz스트림 내용을 읽기
        InputStreamReader streamReader = new InputStreamReader(gzis);

        // 스트림으로 내용을 읽는 경우는 무조건 BufferedReader을 통해 읽자!
        try (BufferedReader lineReader = new BufferedReader(streamReader)) {

            // String 객체보다 처리 속도를 빠르게 하기 위해 StringBuilder 사용
            StringBuilder gzContents = new StringBuilder();

            String line; // 읽은 라인의 값이 저장되는 변수

            String exp = CmmUtil.nvl(pDTO.getExp()); //정규식 표현식
            log.info("exp : " + exp);

            while ((line = lineReader.readLine()) != null) {

                // 정규식 표현에 맞으면 라인 가져오기
                // 10.56.xxx.xxx IP 대역 가져오기
                if (Pattern.matches(exp, line)) {
                    gzContents.append(line + "\n"); //읽은 라인
                }
            }

            // 읽은 전체 내용을 String 타입으로 변경하기
            result = gzContents.toString();

        } catch (IOException e) {
            throw new RuntimeException("Gzip 파일 읽기 실패했습니다.", e);

        }

        gzis.close();
        gzis = null;

        fis.close();
        fis = null;

        streamReader.close();
        streamReader = null;

        log.info(this.getClass().getName() + ".readLocalGzFile End!");

        return result;

    }

    @Override
    public void uploadIP(HadoopDTO pDTO) throws Exception {

    }

    @Override
    public String readHadoopFile(HadoopDTO pDTO) throws Exception {
        return null;
    }
}
