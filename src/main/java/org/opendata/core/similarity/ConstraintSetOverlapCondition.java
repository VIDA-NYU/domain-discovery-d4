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

import org.opendata.core.set.IDSet;

/**
 * Boolean function that allows to determine whether a pair of IDSets overlap
 * significantly.
 * 
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 * @param <T>
 */
public class ConstraintSetOverlapCondition <T extends IDSet> {
    
    private final OverlapSimilarityFunction _func;
    private final SimilarityThresholdCondition _condition;
    private final boolean _isSymmetric;
    
    public ConstraintSetOverlapCondition(
            OverlapSimilarityFunction func,
            SimilarityThresholdCondition condition,
            boolean isSymmetric
    ) {
        _func = func;
        _condition = condition;
        _isSymmetric = isSymmetric;
    }
    
    public boolean isSatisfied(IDSet set1, IDSet set2) {
        
        return _condition.isSatisfied(
                _func.sim(set1.length(), set2.length(), set1.overlap(set2))
        );
    }
    
    public boolean isSymmetric() {
        
        return _isSymmetric;
    }
}
