package kopo.poly.service;

import kopo.poly.dto.HadoopDTO;

public interface IAccessLogUploadService {

    // 네임노드가 설치된 마스터 서버 IP
    String namenodeHost = "192.168.2.136";

    // 네임노드 포트
    String namemodePort = "9000";

    // 얀 포트
    String yarnPort = "8080";

    void uploadFile(HadoopDTO pDTO) throws Exception;

}

