package com.rehoshi.config;

public class TransportConfig {
    //输入源文件的路径
    private String sourceInPath = "/config/In.source";

    //输入源文件路径
    private String sourceOutPath = "/config/Out.source";

    //工作信息路径
    private String jobInfoPath = "/config/Job.info";

    //工作状态目录
    private String jobsHomePath = "/jobs";

    private String logPath = "/logs";

    //是否使用环境变量创建
    public TransportConfig(boolean useEnv) {
        String home = System.getenv("TRANSPORT_HOME");
        if (!useEnv || home == null) {
            this.sourceInPath = TransportConfig.class.getResource(sourceInPath).getPath();
            this.sourceOutPath = TransportConfig.class.getResource(sourceOutPath).getPath();
            this.jobInfoPath = TransportConfig.class.getResource(jobInfoPath).getPath();
            this.jobsHomePath = TransportConfig.class.getResource(jobsHomePath).getPath();
            this.logPath = TransportConfig.class.getResource(logPath).getPath();
        } else {
            this.sourceInPath = home + this.sourceInPath;
            this.sourceOutPath = home + this.sourceOutPath;
            this.jobInfoPath = home + this.jobInfoPath;
            this.jobsHomePath = home + this.jobsHomePath;
            this.logPath = home + this.logPath;
        }
    }

    public String getSourceInPath() {
        return sourceInPath;
    }

    public String getSourceOutPath() {
        return sourceOutPath;
    }

    public String getJobInfoPath() {
        return jobInfoPath;
    }

    public String getJobsHomePath() {
        return jobsHomePath;
    }

    public String getLogPath() {
        return logPath;
    }
}
