package kopo.poly.service;

import kopo.poly.dto.HadoopDTO;

public interface ILogUploadService {

    String readLocalGzFileIP(HadoopDTO pDTO) throws Exception;

}
