package kopo.poly.service;

import kopo.poly.dto.HadoopDTO;

public interface IHdfsFileDownloadService {

    void downloadFile(HadoopDTO pDTO) throws Exception;


}
