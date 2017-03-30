package cn.semicoder.logback.appender;


import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.classic.spi.IThrowableProxy;
import ch.qos.logback.classic.spi.ThrowableProxy;
import ch.qos.logback.core.AppenderBase;
import com.aliyun.openservices.log.common.LogItem;
import com.aliyun.openservices.log.producer.LogProducer;
import com.aliyun.openservices.log.producer.ProducerConfig;
import com.aliyun.openservices.log.producer.ProjectConfig;
import org.apache.commons.lang.StringUtils;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.TimeZone;

/**
 * Created by gavin on 2017/2/14.
 */
public class AliLogAppender extends AppenderBase<ILoggingEvent> {

    private ProducerConfig config = new ProducerConfig();
    private LogProducer producer;
    private ProjectConfig projectConfig = new ProjectConfig();
    private String logstore;
    private String topic = "";
    private String timeZone = "UTC";
    private String timeFormat = "yyyy-MM-dd'T'HH:mmZ";
    private SimpleDateFormat formatter;

    @Override
    protected void append(ILoggingEvent event) {
        List<LogItem> logItems = new ArrayList<LogItem>();
        LogItem item = new LogItem();
        logItems.add(item);
        item.SetTime((int) (event.getTimeStamp() / 1000));
        item.PushBack("time", formatter.format(new Date(event.getTimeStamp())));
        item.PushBack("level", event.getLevel().toString());
        item.PushBack("thread", event.getThreadName());

        item.PushBack("location", getLocation(event));

        Throwable throwable = getThrowable(event);
        String message = event.getFormattedMessage();

        if (StringUtils.isEmpty(message) && throwable != null) {
            message = throwable.toString();
        }

        if (throwable != null) {
            final StringWriter sw = new StringWriter();
            throwable.printStackTrace(new PrintWriter(sw));
            message += sw.toString();
        }

        item.PushBack("message", message);
        producer.send(projectConfig.projectName, logstore, topic, null,
                logItems);
    }

    @Override
    public void start() {
        super.start();
        formatter = new SimpleDateFormat(timeFormat);
        formatter.setTimeZone(TimeZone.getTimeZone(timeZone));
        producer = new LogProducer(config);
        producer.setProjectConfig(projectConfig);
    }

    @Override
    public void stop() {
        super.stop();
        producer.flush();
        producer.close();
    }

    private Throwable getThrowable(ILoggingEvent event) {
        Throwable result = null;
        IThrowableProxy throwableProxy = event.getThrowableProxy();
        if (null != throwableProxy) {
            if (throwableProxy instanceof ThrowableProxy) {
                result = ((ThrowableProxy) throwableProxy).getThrowable();
            }
        }
        return result;
    }

    private String getLocation(ILoggingEvent event) {
        StackTraceElement[] callerData = event.getCallerData();

        StackTraceElement calleeStackTraceElement = null;

        if (null != callerData && callerData.length > 0) {
            calleeStackTraceElement = callerData[0];
        }

        if (calleeStackTraceElement == null) {
            return "";
        }

        String className = calleeStackTraceElement.getClassName();
        String methodName = calleeStackTraceElement.getMethodName();
        String fileName = calleeStackTraceElement.getFileName();
        int lineNumber = calleeStackTraceElement.getLineNumber();

        StringBuffer buf = new StringBuffer();
        buf.append(className);
        buf.append(".");
        buf.append(methodName);
        buf.append("(");
        buf.append(fileName);
        buf.append(":");
        buf.append(lineNumber);
        buf.append(")");

        return buf.toString();
    }

    public boolean requiresLayout() {
        return false;
    }

    public String getTimeFormat() {
        return timeFormat;
    }

    public void setTimeFormat(String timeFormat) {
        this.timeFormat = timeFormat;
        formatter = new SimpleDateFormat(timeFormat);
        formatter.setTimeZone(TimeZone.getTimeZone(timeZone));
    }

    public String getLogstore() {
        return logstore;
    }

    public void setLogstore(String logstore) {
        this.logstore = logstore;
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public String getTimeZone() {
        return timeZone;
    }

    public void setTimeZone(String timeZone) {
        this.timeZone = timeZone;
        formatter = new SimpleDateFormat(timeFormat);
        formatter.setTimeZone(TimeZone.getTimeZone(timeZone));
    }

    public String getProjectName() {
        return projectConfig.projectName;
    }

    public void setProjectName(String projectName) {
        projectConfig.projectName = projectName;
    }

    public String getEndpoint() {
        return projectConfig.endpoint;
    }

    public void setEndpoint(String endpoint) {
        projectConfig.endpoint = endpoint;
    }

    public String getAccessKeyId() {
        return projectConfig.accessKeyId;
    }

    public void setAccessKeyId(String accessKeyId) {
        projectConfig.accessKeyId = accessKeyId;
    }

    public String getAccessKey() {
        return projectConfig.accessKey;
    }

    public void setAccessKey(String accessKey) {
        projectConfig.accessKey = accessKey;
    }

    public String getStsToken() {
        return projectConfig.stsToken;
    }

    public void setStsToken(String stsToken) {
        projectConfig.stsToken = stsToken;
    }

    public int getPackageTimeoutInMS() {
        return config.packageTimeoutInMS;
    }

    public void setPackageTimeoutInMS(int packageTimeoutInMS) {
        config.packageTimeoutInMS = packageTimeoutInMS;
    }

    public int getLogsCountPerPackage() {
        return config.logsCountPerPackage;
    }

    public void setLogsCountPerPackage(int logsCountPerPackage) {
        config.logsCountPerPackage = logsCountPerPackage;
    }

    public int getLogsBytesPerPackage() {
        return config.logsBytesPerPackage;
    }

    public void setLogsBytesPerPackage(int logsBytesPerPackage) {
        config.logsBytesPerPackage = logsBytesPerPackage;
    }

    public int getMemPoolSizeInByte() {
        return config.memPoolSizeInByte;
    }

    public void setMemPoolSizeInByte(int memPoolSizeInByte) {
        config.memPoolSizeInByte = memPoolSizeInByte;
    }

    public int getIoThreadsCount() {
        return config.maxIOThreadSizeInPool;
    }

    public void setIoThreadsCount(int ioThreadsCount) {
        config.maxIOThreadSizeInPool = ioThreadsCount;
    }

    public int getShardHashUpdateIntervalInMS() {
        return config.shardHashUpdateIntervalInMS;
    }

    public void setShardHashUpdateIntervalInMS(int shardHashUpdateIntervalInMS) {
        config.shardHashUpdateIntervalInMS = shardHashUpdateIntervalInMS;
    }

    public int getRetryTimes() {
        return config.retryTimes;
    }

    public void setRetryTimes(int retryTimes) {
        config.retryTimes = retryTimes;
    }

}
