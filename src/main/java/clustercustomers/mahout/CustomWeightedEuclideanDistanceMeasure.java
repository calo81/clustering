package clustercustomers.mahout;

import org.apache.mahout.common.distance.WeightedEuclideanDistanceMeasure;
import org.apache.mahout.math.DenseVector;
import org.apache.mahout.math.Vector;

/**
 * Created with IntelliJ IDEA.
 * User: cscarion
 * Date: 28/05/2014
 * Time: 18:13
 * To change this template use File | Settings | File Templates.
 *
 *
 */
public class CustomWeightedEuclideanDistanceMeasure extends WeightedEuclideanDistanceMeasure {

    public static final Vector WEIGHTS = new DenseVector(new double[]{10.0, 5.0, 1.0, 1.0, 1.0, 1.0/100000, 1.0/5});
    public CustomWeightedEuclideanDistanceMeasure(){
        super();
        setWeights(WEIGHTS);
    }
}
