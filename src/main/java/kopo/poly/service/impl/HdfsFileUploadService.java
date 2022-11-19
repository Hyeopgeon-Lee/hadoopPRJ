package kopo.poly.service.impl;

import kopo.poly.dto.HadoopDTO;
import kopo.poly.service.AbstractHadoopConf;
import kopo.poly.service.IHdfsFileUploadService;
import kopo.poly.util.CmmUtil;
import lombok.extern.log4j.Log4j2;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.List;

@Log4j2
@Service
public class HdfsFileUploadService extends AbstractHadoopConf
        implements IHdfsFileUploadService {

    @Override
    public void uploadHdfsFile(HadoopDTO pDTO) throws Exception {

        log.info(this.getClass().getName() + ".uploadHdfsFile Start!");

        // CentOS에 설치된 하둡 분산 파일 시스템 연결 및 설정하기
        FileSystem hdfs = FileSystem.get(this.getHadoopConfiguration());

        // 하둡에 파일을 업로드할 폴더
        // 예 : /01/02
        String hadoopUploadFilePath = CmmUtil.nvl(pDTO.getHadoopUploadPath());

        // 하둡에 업로드할 파일명
        // 예 : access_log.gz
        String hadoopUploadFileName = CmmUtil.nvl(pDTO.getHadoopUploadFileName());

        // 하둡에 폴더 생성하기
        // hadoop fs -mkdir -p /01/02
        hdfs.mkdirs(new Path(hadoopUploadFilePath));

        // 하둡분산파일시스템에 저장될 파일경로 및 폴더명
        // 예 : hadoop fs -put access_log.gz /01/02/access_log.gz
        String hadoopFile = hadoopUploadFilePath + "/" + hadoopUploadFileName;

        // 하둡분산파일시스템에 저장가능한 객체로 변환
        Path path = new Path(hadoopFile);

        // 기존 하둡에 존재하는지 로그 찍어보기
        log.info("HDFS Exists : " + hdfs.exists(path));

        if (hdfs.exists(path)) { // 하둡분산파일시스템에 파일 존재하면...

            // 기존 업로드되어 있는 파일 삭제하기
            // hadoop fs -rm -r /01/02/access_log.gz
            hdfs.delete(path, true);

        }

        // 예 : c:/hadoop_data/access_log.gz
        Path localPath = new Path(
                CmmUtil.nvl(pDTO.getLocalUploadPath()) +
                        "/" + CmmUtil.nvl(pDTO.getLocalUploadFileName()));

        // 파일 업로드
        // 예 : hadoop fs -put c:/data/access_log.gz /01/02/access_log.gz
        hdfs.copyFromLocalFile(localPath, path);
        hdfs.close();

        log.info(this.getClass().getName() + ".uploadHdfsFile End!");
    }

    @Override
    public void uploadHdfsFileContents(HadoopDTO pDTO) throws Exception {

        log.info(this.getClass().getName() + ".uploadHdfsFileContents Start!");

        // 하둡 분산 파일 시스템 객체
        FileSystem hdfs = null;

        // 하둡 파일에 저장할 내용
        List<String> contentList = pDTO.getContentList();
        log.info("Upload ContentsList Cnt : " + contentList.size());

        // CentOS에 설치된 하둡 분산 파일 시스템 연결 및 설정하기
        hdfs = FileSystem.get(this.getHadoopConfiguration());

        // 하둡에 파일을 업로드할 폴더
        // 예 : /01/02
        String hadoopUploadFilePath = CmmUtil.nvl(pDTO.getHadoopUploadPath());

        // 하둡에 업로드할 파일명
        // 예 : access_log.gz
        String hadoopUploadFileName = CmmUtil.nvl(pDTO.getHadoopUploadFileName());

        // 하둡에 폴더 생성하기
        // hadoop fs -mkdir -p /01/02
        hdfs.mkdirs(new Path(hadoopUploadFilePath));

        // 하둡분산파일시스템에 저장될 파일경로 및 폴더명
        // 예 : hadoop fs -put access_log.gz /01/02/access_log.gz
        String hadoopFile = hadoopUploadFilePath + "/" + hadoopUploadFileName;

        // 하둡분산파일시스템에 저장가능한 객체로 변환
        Path path = new Path(hadoopFile);

        // 기존 하둡에 존재하는지 로그 찍어보기
        log.info("HDFS Exists : " + hdfs.exists(path));

        if (hdfs.exists(path)) { // 하둡분산파일시스템에 파일 존재하면...

            // 기존 업로드되어 있는 파일 삭제하기
            // hadoop fs -rm -r /01/02/access_log.gz
            hdfs.delete(path, true);

        }

        FSDataOutputStream outputStream = hdfs.create(path);

        contentList.forEach(log -> {
            try {
                outputStream.writeUTF(log.trim() + "\n");
            } catch (IOException e) {
                e.printStackTrace();
            }
        });
        outputStream.close();

        log.info(this.getClass().getName() + ".uploadHdfsFileContents End!");
    }
}

