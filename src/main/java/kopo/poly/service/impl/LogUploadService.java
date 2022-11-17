package kopo.poly.service.impl;

import kopo.poly.dto.HadoopDTO;
import kopo.poly.service.AbstractHadoopConf;
import kopo.poly.service.ILogUploadService;
import kopo.poly.util.CmmUtil;
import lombok.extern.log4j.Log4j2;
import org.springframework.stereotype.Service;

import java.io.*;
import java.util.regex.Pattern;
import java.util.zip.GZIPInputStream;

@Log4j2
@Service
public class LogUploadService extends AbstractHadoopConf implements ILogUploadService {

    @Override
    public String readLocalGzFileIP(HadoopDTO pDTO) throws Exception {
        log.info(this.getClass().getName() + ".readLocalGzFile Start!");

        String result = ""; // GzFile 내용 결과

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
            log.info("exp : " + exp); // 예 : 10\.223\.[0-9]{1,3}\.[0-9]{1,3} => 10.56.xxx.xxx 값만 찾음

            while ((line = lineReader.readLine()) != null) {

                String ip = line.substring(0, line.indexOf(" ")); //IP값만 가져오기

                // 정규식 표현에 맞으면 라인 가져오기
                // 10.56.xxx.xxx IP 대역 가져오기
                // 자바 정규식은 CentOS의 grep 명령어와 달리 비교대상과 정규식 패턴이 일치해야만 탐지 가능
                // 즉, 로그에서 IP를 추출하고, 그 IP에 대한 정규식 패턴이 맞는지 체크함
                if (Pattern.matches(exp, ip)) {
                    gzContents.append(line + "\n"); // 비교는 IP랑 하지만 실제 HDFS에 업로드내용은 한줄 전체

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
