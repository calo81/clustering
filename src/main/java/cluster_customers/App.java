package cluster_customers;


import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.mahout.clustering.Cluster;
import org.apache.mahout.clustering.classify.WeightedPropertyVectorWritable;
import org.apache.mahout.clustering.fuzzykmeans.FuzzyKMeansClusterer;
import org.apache.mahout.clustering.kmeans.KMeansDriver;
import org.apache.mahout.clustering.kmeans.Kluster;
import org.apache.mahout.clustering.kmeans.RandomSeedGenerator;
import org.apache.mahout.common.distance.DistanceMeasure;
import org.apache.mahout.common.distance.EuclideanDistanceMeasure;
import org.apache.mahout.common.distance.WeightedEuclideanDistanceMeasure;
import org.apache.mahout.math.DenseVector;
import org.apache.mahout.math.NamedVector;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;
import org.apache.mahout.utils.clustering.ClusterDumper;

import java.io.File;
import java.util.ArrayList;
import java.util.List;


public class App {
    public static void main(String[] args) throws Exception {

        /** First we assume structure like:
         * (plumber, electrician, timeworking)
         */


        
        List<NamedVector> customers = new ArrayList<NamedVector>();

        NamedVector customer;
        customer = new NamedVector(
                new DenseVector(new double[]{10, 0, 1}),
                "john:123");
        customers.add(customer);
        customer = new NamedVector(
                new DenseVector(new double[]{10, 0, 5}),
                "karl:456");
        customers.add(customer);
        customer = new NamedVector(
                new DenseVector(new double[]{10, 0, 3}),
                "lindsay:7468");
        customers.add(customer);
        customer = new NamedVector(
                new DenseVector(new double[]{0, 10, 2}),
                "riggs:9837");
        customers.add(customer);
        customer = new NamedVector(
                new DenseVector(new double[]{0, 10, 4}),
                "lopez:8634");

        customers.add(customer);
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);

        Path path = new Path("customerdata/customers");
        SequenceFile.Writer writer = new SequenceFile.Writer(fs, conf,
                path, Text.class, VectorWritable.class);
        VectorWritable vec = new VectorWritable();
        for (NamedVector vector : customers) {
            vec.set(vector);
            writer.append(new Text(vector.getName()), vec);
        }
        writer.close();

        SequenceFile.Reader reader = new SequenceFile.Reader(fs,
                new Path("customerdata/customers"), conf);

        Text key = new Text();
        VectorWritable value = new VectorWritable();
        while (reader.next(key, value)) {
            System.out.println(key.toString() + " " + value.get().asFormatString());
        }
        reader.close();


        Path output = new Path("customerdata/output");
        FileUtils.deleteDirectory(new File(output.toString()));
        Path clustersIn = new Path(output, "random-seeds");
        DistanceMeasure measure = new EuclideanDistanceMeasure();


        RandomSeedGenerator.buildRandom(conf, path, clustersIn, 2, measure);
        KMeansDriver.run(conf, path, clustersIn, output, 0.01, 10, true,
                0.0, true);

        List<List<Cluster>> Clusters = ClusterHelper.readClusters(conf, output);

        for (Cluster cluster : Clusters.get(Clusters.size() - 1)) {
            System.out.println("Cluster id: " + cluster.getId() + " center: "
                    + cluster.getCenter().asFormatString());

        }




        SequenceFile.Reader reader2 = new SequenceFile.Reader(fs,
                new Path("customerdata/output/" + Cluster.CLUSTERED_POINTS_DIR + "/part-m-0"), conf);

        IntWritable key2 = new IntWritable();
        WeightedPropertyVectorWritable value2 = new WeightedPropertyVectorWritable();
        while (reader2.next(key2, value2)) {
            NamedVector nVec = (NamedVector) value2.getVector();
            //if (String.valueOf(clusterIdBelong).equals(key.toString())) {
            System.out.println("ID: " + nVec + "  " + value2.toString() + " belongs to cluster " + key2.toString());
            //}
        }
        reader2.close();



    }
}
