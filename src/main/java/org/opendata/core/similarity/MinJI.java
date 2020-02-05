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
package org.opendata.core.similarity;

import java.math.BigDecimal;
import org.opendata.core.util.FormatedBigDecimal;

/**
 * BigDecimal for Jaccard-Index over the smaller of two sets.
 * 
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class MinJI extends FormatedBigDecimal implements Comparable<MinJI>, OverlapSimilarityFunction {
    
    public MinJI(int overlap, int setSize1, int setSize2, int scale) {
        
        super(new BigDecimal((double)overlap/(double)Math.min(setSize1, setSize2)), scale);
    }
    
    public MinJI(int overlap, int setSize1, int setSize2) {
        
        super(new BigDecimal((double)overlap/(double)Math.min(setSize1, setSize2)));
    }
    
    public MinJI() {
        
        super(BigDecimal.ZERO);
    }

    @Override
    public int compareTo(MinJI r) {

        return this.value().compareTo(r.value());
    }

    @Override
    public double sim(int size1, int size2, int overlap) {

        return ((double)overlap/(double)size1);
    }
}
