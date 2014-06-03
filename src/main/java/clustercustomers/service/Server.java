package clustercustomers.service;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import clustercustomers.hadoop.fakes.QuestionsAnswers;
import clustercustomers.mahout.CustomWeightedEuclideanDistanceMeasure;
import clustercustomers.sqoop.QueryResult;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.mahout.clustering.Cluster;
import org.apache.mahout.clustering.iterator.ClusterWritable;
import org.apache.mahout.math.DenseVector;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;
import org.simpleframework.http.Request;
import org.simpleframework.http.Response;
import org.simpleframework.http.core.Container;
import org.simpleframework.http.core.ContainerServer;
import org.simpleframework.transport.connect.Connection;
import org.simpleframework.transport.connect.SocketConnection;

public class Server implements Container {

    private final Map<Integer, String> commonInsurers;
    private final Map<Integer, String> commonQuestions;
    private Configuration conf = new Configuration();
    private List<Vector> centroids;
    private Map<Vector, Cluster> clusters = new HashMap<Vector, Cluster>();
    private FileSystem fs = null;
    private Map<Integer, Double> premiumAverages;

    public Server() throws Exception {
        conf.set("fs.default.name", "hdfs://192.168.1.10:9000/");
        conf.set("mapred.job.tracker", "192.168.1.10:9001");
        fs = FileSystem.get(conf);
        centroids = loadCentroids();
        premiumAverages = loadPremiumAverages();
        commonInsurers = loadCommonInsurer();
        commonQuestions = loadCommonQuestions();
        System.out.println("Server started");
    }

    private Map<Integer, String> loadCommonQuestions() throws Exception {
        final Map<Integer, String> commonQuestions = new HashMap<Integer, String>();
        FileStatus[] files = fs.globStatus(new Path("/user/cscarion/commonQuestionAggregate/part-r-00000"));
        withFiles(files, new Executor() {
            @Override
            public void execute(String... fields) {
                commonQuestions.put(Integer.parseInt(fields[0]), fields[1]);
            }
        });
        return commonQuestions;
    }

    private Map<Integer, Double> loadPremiumAverages() throws Exception {
        final Map<Integer, Double> averages = new HashMap<Integer, Double>();
        FileStatus[] files = fs.globStatus(new Path("/user/cscarion/premiumsAverage/part-r-00000"));

        withFiles(files, new Executor() {
            @Override
            public void execute(String... fields) {
                averages.put(Integer.parseInt(fields[0]), Double.parseDouble(fields[1]));
            }
        });
        return averages;
    }

    private Map<Integer, String> loadCommonInsurer() throws Exception {
        final Map<Integer, String> commonInsurer = new HashMap<Integer, String>();
        FileStatus[] files = fs.globStatus(new Path("/user/cscarion/commonInsurerAggregate/part-r-00000"));
        withFiles(files, new Executor() {
            @Override
            public void execute(String... fields) {
                commonInsurer.put(Integer.parseInt(fields[0]), fields[1]);
            }
        });
        return commonInsurer;
    }

    private void withFiles(FileStatus[] files, Executor executor) throws Exception {
        for (FileStatus file : files) {
            if (file.getLen() > 0) {
                FSDataInputStream in = fs.open(file.getPath());
                BufferedReader bin = new BufferedReader(new InputStreamReader(
                        in));
                String s = bin.readLine();
                while (s != null) {
                    String[] fields = s.split("\\|");
                    executor.execute(fields);
                    s = bin.readLine();
                }
                in.close();
            }
        }
    }


    private List<Vector> loadCentroids() throws Exception {
        List<Vector> centroids = new ArrayList<Vector>();
        SequenceFile.Reader reader = new SequenceFile.Reader(fs,
                new Path("/user/cscarion/customer-centroids/clusters-0-final/part-r-00000"), conf);
        Text key = new Text();
        ClusterWritable value = new ClusterWritable();


        while (reader.next(key, value)) {
            centroids.add(value.getValue().getCenter());
            clusters.put(value.getValue().getCenter(), value.getValue());
        }
        return centroids;
    }

    public void handle(Request request, Response response) {
        if (request.getPath().getPath().contains("questions")) {
            questionsReturn(request, response);
        } else {
            comparisonSummary(request, response);
        }
    }

    private void questionsReturn(Request request, Response response) {
        try {
            Cluster cluster = getRelevantCluster(request);
            String commonQuestion = commonQuestions.get(cluster.getId());
            String commonAnswer = QuestionsAnswers.QUESTIONS_ANSWERS_MAP.get(commonQuestion);
            sendQuestionResponse(response, commonQuestion, commonAnswer, cluster);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void sendQuestionResponse(Response response, String commonQuestion, String commonAnswer, Cluster cluster) throws Exception{
        long time = System.currentTimeMillis();
        response.setValue("Content-Type", "text/html");
        response.setValue("Server", "HelloWorld/1.0 (Simple 4.0)");
        response.setValue("Access-Control-Allow-Origin", "*");
        response.setDate("Date", time);
        response.setDate("Last-Modified", time);

        String html = "<div style=\"width: 100%; float: left;\">\n" +
                "  <p>\n" +
                "\t  <ul>\n" +
                "\t<li>(For development) Belong to cluster: " + cluster.getId() + "</li>\n" +
                "\t<li><b>" + commonQuestion + "<b/></li>\t\n" +
                "\t\t   -----  " + commonAnswer + "\t\n" +
                "   </ul>\n" +
                "  </p>\n" +
                "</div>";


        response.getPrintStream().print(html);
        response.close();
    }

    private void comparisonSummary(Request request, Response response) {
        try {
            Cluster cluster = getRelevantCluster(request);
            double claimAverage = getClaimAverageForCluster(cluster);
            String commonInsurer = commonInsurers.get(cluster.getId());
            String commonQuestion = commonQuestions.get(cluster.getId());
            String commonAnswer = QuestionsAnswers.QUESTIONS_ANSWERS_MAP.get(commonQuestion);
            sendResponse(response, claimAverage, commonInsurer, commonQuestion, commonAnswer, cluster);
        } catch (Exception e) {
            e.printStackTrace();
            try {
                response.close();
            } catch (IOException e1) {
                e1.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
            }
        }
    }

    private Cluster getRelevantCluster(Request request) {
        Vector newValue = toVector(request.getParameter("vector"));
        double distance = Double.MAX_VALUE;
        Vector selectedCentroid = null;
        for (Vector centroid : centroids) {
            if (distance(centroid, newValue) < distance) {
                distance = distance(centroid, newValue);
                selectedCentroid = centroid;
            }
        }
        return clusters.get(selectedCentroid);
    }

    private static abstract class Executor {
        public abstract void execute(String... fields);
    }

    /**
     * This is copied from the WeightedEuclideanDistanceMeasure class in Mahout. Just for Spike purposes.
     *
     * @param centroid
     * @param v
     * @return
     */
    private double distance(Vector centroid, Vector v) {
        double result = 0;
        Vector res = v.minus(centroid);
        Vector theWeights = getWeights();
        if (theWeights == null) {
            for (Vector.Element elt : res.nonZeroes()) {
                result += elt.get() * elt.get();
            }
        } else {
            for (Vector.Element elt : res.nonZeroes()) {
                result += elt.get() * elt.get() * theWeights.get(elt.index());
            }
        }
        return Math.sqrt(result);
    }

    private Vector getWeights() {
        return CustomWeightedEuclideanDistanceMeasure.WEIGHTS;
    }

    private void sendResponse(Response response, double claimAverage, String commonInsurer, String commonQuestion, String commonAnswer, Cluster cluster) throws IOException {
        long time = System.currentTimeMillis();
        response.setValue("Content-Type", "text/html");
        response.setValue("Server", "HelloWorld/1.0 (Simple 4.0)");
        response.setValue("Access-Control-Allow-Origin", "*");
        response.setDate("Date", time);
        response.setDate("Last-Modified", time);

        String html = "<div style=\"width: 100%; float: left;\">\n" +
                "  <h3 class='ribbon'>Similar customers to you are insuring with us:<h3/>\n" +
                "  <p>\n" +
                "\t  <ul>\n" +
                "\t<li>(For development) Belong to cluster: " + cluster.getId() + "</li>\n" +
                "\t<li>The premiums offered average " + claimAverage + "</li>\n" +
                "\t<li>They more often insure with " + commonInsurer + "</li>\t\n" +
                "\t<li>Their most asked question is " + commonQuestion + "</li>\t\n" +
                "   </ul>\n" +
                "  </p>\n" +
                "</div>";


        response.getPrintStream().print(html);
        response.close();
    }

    private double getClaimAverageForCluster(Cluster cluster) throws Exception {
        return premiumAverages.get(cluster.getId());
    }

    private Vector toVector(String vector) {
        System.out.println("Received Vector: " + vector);
        String[] coordinates = vector.split(",");
        double[] doubleCoordinates = new double[coordinates.length];
        for (int i = 0; i < coordinates.length; i++) {
            doubleCoordinates[i] = Double.parseDouble(coordinates[i]);
        }
        return new DenseVector(doubleCoordinates);
    }

    public static void main(String[] list) throws Exception {
        Container container = new Server();
        org.simpleframework.transport.Server server = new ContainerServer(container);
        Connection connection = new SocketConnection(server);
        SocketAddress address = new InetSocketAddress(8080);

        connection.connect(address);
    }
}
