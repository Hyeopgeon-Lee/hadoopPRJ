/*
 * 하둡 접속 설정
 * */
public Configuration getHadoopConfiguration() {

        Configuration conf = new Configuration();

        // fs.defaultFS 설정 값 : hdfs://192.168.2.136:9000
        conf.set("fs.defaultFS", "hdfs://" + IAccessLogUploadService.namenodeHost + ":"
        + IAccessLogUploadService.namemodePort);

        // yarn 주소 설정
        conf.set("yarn.resourcemanager.address", IAccessLogUploadService.namenodeHost + ":"
        + IAccessLogUploadService.yarnPort);

        return conf;
        }
        }