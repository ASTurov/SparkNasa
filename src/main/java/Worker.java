import org.apache.spark.SparkContext;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.*;

import java.io.File;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Locale;

import static org.apache.spark.api.java.JavaSparkContext.toSparkContext;
import static org.apache.spark.sql.functions.count;
import static org.apache.spark.sql.functions.window;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public final class Worker {

    static public String g_PatternRowData = "(.*)( - - )(.*])(\\s)(.*\")(\\s)(.*)(\\s)(.*$)";
    static DateTimeFormatter g_PatternFileTime = DateTimeFormatter.ofPattern("dd/MMM/yyyy:HH:mm:ss Z", Locale.UK);
    static DateTimeFormatter g_PatternSparkTime = DateTimeFormatter.ofPattern("yyyy-MM-dd");

    static int flag = 0;

    private static MapFunction<String, LogData> String2LogDataMapper = data -> {
        Pattern r = Pattern.compile(g_PatternRowData);
        Matcher m = r.matcher(data);
        String Host = new String();
        String Timestamp = new String();
        String Request = new String();
        String Reply = new String();
        String ColumnByteReply= new String();
        if (m.find()) {
            Host = m.group(1);
            Timestamp = m.group(3);

            int startTime = Timestamp.indexOf('[');
            int endTime = Timestamp.indexOf(']');
            Timestamp = Timestamp.substring(startTime + 1, endTime);
            Request = m.group(5);
            Reply = m.group(7);
            ColumnByteReply = m.group(9);
        }

        Timestamp = g_PatternSparkTime.format(ZonedDateTime.parse(Timestamp, g_PatternFileTime));

        LogData out = new LogData();

        out.setHost(Host);
        out.setTimestamp(Timestamp);
        out.setReply(Reply);
        out.setRequest(Request);
        out.setColumnByteReply(ColumnByteReply);

        return out;
    };

    static Function<LogData, Boolean> filterMore5xx = e -> (Integer.parseInt(e.getReply()) > 500 && Integer.parseInt(e.getReply()) < 600);

    public static void start(final SparkConf conf, final JavaSparkContext context, final String inputFile, final String outputDir)
    {
        SparkSession spark = SparkSession
                .builder()
                .sparkContext(toSparkContext(context))
                .getOrCreate();

        File tmpFile = new File(inputFile);
        if (!tmpFile.exists()) {
            throw new IllegalArgumentException("Invalid arguments");
        }
        JavaRDD<Row> myData = spark.read().textFile(inputFile)
                .map(String2LogDataMapper, Encoders.bean(LogData.class))
                .filter("reply >= 500").filter("reply < 600")
                .groupBy("request", "host")
                .agg(count("request").as("count")).coalesce(1).toJavaRDD();
        myData.saveAsTextFile(outputDir + "/task1");


        Dataset<Row> myData2 =  spark
                .read().textFile(inputFile)
                .map(String2LogDataMapper, Encoders.bean(LogData.class))
                .groupBy("timestamp", "request", "reply").agg(count("request")
                .as("count"))
                .filter("count >= 10")
                .orderBy("timestamp");
        myData2.coalesce(1).toJavaRDD().saveAsTextFile(outputDir + "/task2");

        Dataset<LogData> ds = spark
                .read().textFile(inputFile)
                .map(String2LogDataMapper, Encoders.bean(LogData.class));
//
        Dataset<Row> myData3 = ds.filter("reply >= 1").filter("reply < 600")
                .groupBy(window(ds.col("timestamp").as("timestamp"), "1 week", "1 day" ))
                .agg(count("host").as("weekly_avg"))
                .select("window.start", "window.end", "weekly_avg")
                .sort("start");
                //.map(resultMapper, Encoders.STRING())
        myData3.coalesce(1).toJavaRDD().saveAsTextFile(outputDir + "/task3");
    }
}