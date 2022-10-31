package com.rison.query.job;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * @PACKAGE_NAME: com.rison.query.job
 * @NAME: SparkSQLJob
 * @USER: Rison
 * @DATE: 2022/10/31 10:08
 * @PROJECT_NAME: bigdata-query-handler
 **/
public class SparkSQLJob {
    private static final Logger LOGGER = LoggerFactory.getLogger(SparkSQLJob.class);

    public static void main(String[] args) throws IOException{
        SparkSession spark;
        String authentication = args[0];
        final Configuration conf = new Configuration();
        String confDir = args[4];
        String keytab = args[5];
        String principal = args[6];
        conf.addResource(new Path(confDir + "/hdfs/hdfs-site.xml"));
        conf.addResource(new Path(confDir + "/hdfs/core-site.xml"));
        if (authentication.equalsIgnoreCase("tbds")){
            conf.set("hadoop.security.authentication", authentication);
            conf.set("hadoop_security_authentication_tbds_username", args[1]);
            conf.set("hadoop_security_authentication_tbds_secureid", args[2]);
            conf.set("hadoop_security_authentication_tbds_securekey", args[3]);
            UserGroupInformation.setConfiguration(conf);
            UserGroupInformation.loginUserFromSubject(null);
        } else if (authentication.equalsIgnoreCase("kerberos")){
            System.setProperty("java.security.krb5.conf", "/etc/krb5.conf");
            conf.set("haddoop.security.authentication", authentication);
            UserGroupInformation.loginUserFromKeytab(principal, keytab);
        }
        String detailId = args[7];
        String sparkQueueName = args[8];
        String databaseName = args[9];
        String scripContent = args[10];

        if ("kerberos".equalsIgnoreCase(authentication)){
            spark = SparkSession.builder()
                    .appName("mySparkSession")
                    .master("yarn")
                    .config("spark.yarn.queue", sparkQueueName)
                    .config("spark.kerberos.keytab", keytab)
                    .config("spark.kerberos.principal", principal)
                    .enableHiveSupport()
                    .getOrCreate();
        }else {
            spark = SparkSession.builder()
                    .appName("mySparkSession")
                    .master("yarn")
                    .config("spark.yarn.queue", sparkQueueName)
                    .enableHiveSupport()
                    .getOrCreate();
        }
        spark.sql("use " + databaseName).show();
        Dataset<Row> dataset = spark.sql(scripContent);
        dataset.show();
        final FileSystem fileSystem = FileSystem.get(conf);
        final String dataSaveDir = "/tmp/sparkSqlData/" + detailId;
        final Path dataSavePath = new Path(dataSaveDir);
        if (!fileSystem.exists(dataSavePath)){
            fileSystem.mkdirs(dataSavePath);
        }
        if (dataset != null && dataset.count() != 0L){
            dataset.repartition(1)
                    .write()
                    .format("json")
                    .mode(SaveMode.Overwrite)
                    .save("/tmp/sparkSqlData/" + detailId);
        }
        spark.stop();
        fileSystem.close();

    }
}
