package kopo.poly.service.impl;

import kopo.poly.dto.HadoopDTO;
import kopo.poly.service.AbstractHadoopConf;
import kopo.poly.service.IHdfsFileDownloadService;
import kopo.poly.util.CmmUtil;
import lombok.extern.log4j.Log4j2;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.springframework.stereotype.Service;

import java.io.IOException;

@Log4j2
@Service
public class HdfsFileDownloadService extends AbstractHadoopConf implements IHdfsFileDownloadService {

    @Override
    public void downloadFile(HadoopDTO pDTO) throws Exception {

        log.info(this.getClass().getName() + ".downloadFile Start!");

        // 하둡 분산 파일 시스템 객체
        FileSystem hdfs = null;

        try {
            // CentOS에 설치된 하둡 분산 파일 시스템 연결 및 설정하기
            hdfs = FileSystem.get(this.getHadoopConfiguration());

            // 다운로드하기 위한 하둡에 저장된 파일의 폴더
            // 예 : /01/02
            String hadoopUploadFilePath = CmmUtil.nvl(pDTO.getHadoopUploadPath());

            // 다운로드하기 위한 하둡에 저장된 파일명
            // 예 : access_log.gz
            String hadoopUploadFileName = CmmUtil.nvl(pDTO.getHadoopUploadFileName());

            // 하둡분산파일시스템에 저장될 파일경로 및 폴더명
            // 예 : hadoop fs -put access_log.gz /01/02/access_log.gz
            String hadoopFile = hadoopUploadFilePath + "/" + hadoopUploadFileName;

            // 하둡분산파일시스템에 저장가능한 객체로 변환
            Path path = new Path(hadoopFile);

            // 기존 하둡에 존재하는지 로그 찍어보기
            log.info("HDFS Exists : " + hdfs.exists(path));

            if (hdfs.exists(path)) { // 하둡분산파일시스템에 파일 존재하면...


                // 다운르도 파일명 예 : c:/hadoop_data/hdfs_access_log.gz
                Path localPath = new Path(
                        CmmUtil.nvl(pDTO.getLocalUploadPath()) +
                                "/" + CmmUtil.nvl(pDTO.getLocalUploadFileName()));

                // 업로드된 파일 다운로드하기
                // hadoop fs -get /01/02/access_log.gz c:/hadoop_data/hdfs_access_log.gz
                hdfs.copyToLocalFile(path, localPath);

            }

        } catch (IOException e) {
            log.info(e.getMessage());

        } finally {
            if (hdfs != null) {
                hdfs.close();

            }
            log.info(this.getClass().getName() + ".downloadFile End!");
        }
    }

}
