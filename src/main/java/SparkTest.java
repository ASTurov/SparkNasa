import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

public class SparkTest
{

    public static void SparkStart(final String input, final String output) throws Exception
    {
        final SparkConf sparkConf = new SparkConf().setAppName("SparkTest").setMaster("local");
        final JavaSparkContext sc = new JavaSparkContext(sparkConf);

        Worker.start(sparkConf, sc, input, output);

    }
    public static void main(String[] args) throws Exception {
        try {
            if (args.length < 2)
            {
                throw new IllegalArgumentException("Invalid arguments");
            }
            SparkStart(("hdfs://" + args[0] + ":9000" + args[1], "hdfs://" + args[0] + ":9000" );
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}

