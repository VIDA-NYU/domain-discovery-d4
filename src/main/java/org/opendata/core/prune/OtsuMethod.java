/*
 * This file is part of the Data-Driven Domain Discovery Tool (D4).
 * 
 * Copyright (c) 2018-2020 New York University.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.opendata.core.prune;

import java.util.List;
import org.opendata.core.object.IdentifiableDouble;
import org.opendata.core.util.Bin;
import org.opendata.core.util.SimilarityHistogram;

/**
 * Candidate set finder that uses the Otsu method to determine the pruning
 * threshold.
 * 
 * Adopted from http://www.labbookpages.co.uk/software/imgProc/otsuThreshold.html
 * 
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 * @param <T>
 */
public class OtsuMethod <T extends IdentifiableDouble> {

    private final int _scale;
    private final SizeFunction _sizeFunc;
    
    public OtsuMethod(SizeFunction sizeFunc, int scale) {
    
        _sizeFunc = sizeFunc;
        _scale = scale;
    }
    
    public OtsuMethod(SizeFunction sizeFunc) {
        
        this(sizeFunc, 3);
    }
    
    public int getPruneIndex(List<T> elements, int start) {

        SimilarityHistogram histogram = new SimilarityHistogram(_scale);
        for (int iEl = start; iEl < elements.size(); iEl++) {
            T el = elements.get(iEl);
            histogram.add(el.value(), _sizeFunc.getSize(el.id()));
        }
        List<Bin> bins = histogram.bins();
        int t = OtsuMethod.getThreshold(bins, histogram.totalSize());
        double threshold = bins.get(t).start();

        int index = 0;
        for (T el : elements) {
            if (el.value() >= threshold) {
                index++;
            } else {
                break;
            }
        }
        return index;
    }
    

    public static int getThreshold(List<Bin> bins, long total) {

        float sum = 0;
        for (int t = 0; t < bins.size(); t++) {
            sum += t * bins.get(t).count();
        }

        float sumB = 0;
        long wB = 0;

        float varMax = 0;
        int threshold = 0;

        for (int t = 0; t < bins.size(); t++) {
            wB += bins.get(t).count(); // Weight Background
            if (wB == 0) {
                continue;
            }

            long wF = total - wB; // Weight Foreground
            if (wF == 0) {
                break;
            }
            sumB += (float) (t * bins.get(t).count());

            float mB = sumB / wB; // Mean Background
            float mF = (sum - sumB) / wF; // Mean Foreground

            // Calculate Between Class Variance
            float varBetween = (float) wB * (float) wF * (mB - mF) * (mB - mF);

            // Check if new maximum found
            if (varBetween > varMax) {
                varMax = varBetween;
                threshold = t;
            }
        }
    
        return threshold;
    }
}
