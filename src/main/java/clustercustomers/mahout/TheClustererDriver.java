package clustercustomers.mahout;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.mahout.clustering.canopy.CanopyDriver;
import org.apache.mahout.clustering.kmeans.KMeansDriver;
import org.apache.mahout.common.distance.EuclideanDistanceMeasure;
import org.apache.mahout.common.distance.TanimotoDistanceMeasure;

/**
 * Created with IntelliJ IDEA.
 * User: cscarion
 * Date: 28/05/2014
 * Time: 18:35
 * To change this template use File | Settings | File Templates.
 */
public class TheClustererDriver {
    public static void main(String[] args) throws Exception{
        Configuration conf = new Configuration();
        conf.set("fs.default.name", "hdfs://192.168.1.10:9000/");
        conf.set("mapred.job.tracker", "192.168.1.10:9001");
        Path vectorsFolder = new Path("vector_seq_file");
        Path canopyCentroids = new Path("customer-centroids");
        Path clusterOutput = new Path("customer-kmeans");

        CanopyDriver.run(conf, vectorsFolder, canopyCentroids,
                new EuclideanDistanceMeasure(), 250.0, 120.0,
                false,0.10, false);

        KMeansDriver.run(conf, vectorsFolder,
                new Path(canopyCentroids, "clusters-0"),
                clusterOutput, 0.01,
                20, true, 0.1,false);
    }
}
