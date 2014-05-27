package cluster_customers;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import org.apache.mahout.common.RandomUtils;
import org.apache.mahout.math.NamedVector;
import org.apache.mahout.math.Vector;

public class RandomPointsUtil {

    public static List<Vector> chooseRandomPoints(Iterable<NamedVector> vectors, int k) {
        List<Vector> chosenPoints = new ArrayList<Vector>(k);
        Random random = RandomUtils.getRandom();
        for (Vector value : vectors) {
            int currentSize = chosenPoints.size();
            if (currentSize < k) {
                chosenPoints.add(value);
            } else if (random.nextInt(currentSize + 1) == 0) { // with chance 1/(currentSize+1) pick new element
                int indexToRemove = random.nextInt(currentSize); // evict one chosen randomly
                chosenPoints.remove(indexToRemove);
                chosenPoints.add(value);
            }
        }
        return chosenPoints;
    }
}
