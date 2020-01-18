package org.apache.mahout.cf.taste.impl.similarity;

import com.google.common.base.Preconditions;
import org.apache.mahout.cf.taste.common.TasteException;
import org.apache.mahout.cf.taste.common.Weighting;
import org.apache.mahout.cf.taste.model.DataModel;

/**
 * author:zwy
 * Date:2020-01-18
 * Time:13:53
 */
public final class CustomPearsonCorrelationSimilarity extends AbstractSimilarity {

    private double k ;

    public CustomPearsonCorrelationSimilarity(DataModel dataModel) throws TasteException {
        this(dataModel, Weighting.UNWEIGHTED);
    }

    public CustomPearsonCorrelationSimilarity(DataModel dataModel,double k) throws TasteException {
        this(dataModel, Weighting.UNWEIGHTED);
        this.k = k;
    }

    public CustomPearsonCorrelationSimilarity(DataModel dataModel, Weighting weighting) throws TasteException {
        super(dataModel, weighting, true);
        Preconditions.checkArgument(dataModel.hasPreferenceValues(), "DataModel doesn't have preference values");
    }

    double computeResult(int n, double sumXY, double sumX2, double sumY2, double sumXYdiff2) {
        if (n == 0) {
            return 0.0D / 0.0;
        } else {
            double denominator = Math.sqrt(sumX2) * Math.sqrt(sumY2);
            double result = denominator == 0.0D ? 0.0D / 0.0 : sumXY / denominator;
            if (result>this.k){
                return result;
            }else {
                return 0D;
            }

        }
    }
}
