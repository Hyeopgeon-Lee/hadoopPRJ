package kopo.poly.service.impl;

import kopo.poly.dto.HadoopDTO;
import kopo.poly.service.ILocalGzFileReadService;
import kopo.poly.util.CmmUtil;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;
import java.util.zip.GZIPInputStream;

@Slf4j
@Service
public class LocalGzFileReadService implements ILocalGzFileReadService {

    @Override
    public List<String> readLocalGzFileCnt(HadoopDTO pDTO) throws Exception {
        log.info(this.getClass().getName() + ".readLocalGzFile Start!");

        // 읽을 라인수
        // 예 : 값이 10이면 10줄만 읽기
        long readCnt = pDTO.getLineCnt();
        log.info("readCnt : " + readCnt);

        List<String> logList = new ArrayList<>(); //로그 정보를 저장할 객체

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

            long idx = 0; //읽은 라인 횟수

            String line; // 읽은 라인의 값이 저장되는 변수

            while ((line = lineReader.readLine()) != null) {

                log.info("line : "+ line);
                logList.add(line);

                idx++; // 읽은 라인 횟수 세기

                // 읽을 라인수까지 읽으면 더 이상 파일 내용을 읽지 않음
                if (readCnt == idx) {
                    break;
                }
            }
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

        return logList;
    }

    @Override
    public List<String> readLocalGzFileIP(HadoopDTO pDTO) throws Exception {
        log.info(this.getClass().getName() + ".readLocalGzFile Start!");

        // 예 : c:/hadoop_data/access_log.gz
        String localFile = CmmUtil.nvl(pDTO.getLocalUploadPath()) +
                "/" + CmmUtil.nvl(pDTO.getLocalUploadFileName());

        log.info("localFile : " + localFile);

        List<String> logList = new ArrayList<>(); //로그 정보를 저장할 객체

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

            String line; // 읽은 라인의 값이 저장되는 변수

            String exp = CmmUtil.nvl(pDTO.getRegExp()); //정규식 표현식
            log.info("exp : " + exp); // 예 : 10\.56\.[0-9]{1,3}\.[0-9]{1,3} => 10.56.xxx.xxx 값만 찾음

            while ((line = lineReader.readLine()) != null) {

                String ip = line.substring(0, line.indexOf(" ")); //IP값만 가져오기

                // 정규식 표현에 맞으면 라인 가져오기
                // 10.56.xxx.xxx IP 대역 가져오기
                // 자바 정규식은 CentOS의 grep 명령어와 달리 비교대상과 정규식 패턴이 일치해야만 탐지 가능
                // 즉, 로그에서 IP를 추출하고, 그 IP에 대한 정규식 패턴이 맞는지 체크함
                if (Pattern.matches(exp, ip)) {
                    logList.add(line); // 정규식 패턴에 맞는 로그 라인 전체 List<String> 객체에 저장
                }
            }
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

        return logList;
    }
}
