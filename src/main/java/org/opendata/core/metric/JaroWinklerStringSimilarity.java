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
package org.opendata.core.metric;

import java.math.BigDecimal;
import org.apache.commons.text.similarity.JaroWinklerDistance;
import org.opendata.core.constraint.Threshold;

/**
 * Compute string similarity as Jaro-Winkler distance between the given terms.
 * 
 * Takes a minimum threshold constraint as parameter. Returns null if the
 * distance between the two strings does not satisfy the constraint.
 * 
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class JaroWinklerStringSimilarity implements StringSimilarityComputer {

    private final Threshold _threshold;
    
    public JaroWinklerStringSimilarity(Threshold threshold) {
        
        _threshold = threshold;
    }
    
    @Override
    public BigDecimal sim(String term1, String term2) {

        BigDecimal sim;
        sim = new BigDecimal(new JaroWinklerDistance().apply(term1, term2));
        
        if (_threshold.isSatisfied(sim)) {
            return sim;
        } else {
            return null;
        }
    }
}
