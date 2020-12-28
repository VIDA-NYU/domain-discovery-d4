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
package org.opendata.db.eq.similarity;

import java.math.BigDecimal;

/**
 * Similarity score for a pair of equivalence classes. Maintains the overlap
 * between the two equivalence classes and the similarity score. The overlap
 * is maintained by some data structures to avoid repeated overlap computation
 * in an interactive setting.
 * 
 * @author @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class SimilarityScore {
   
    private final int _overlap;
    private final BigDecimal _score;
    
    public SimilarityScore(int overlap, BigDecimal score) {
    
        _overlap = overlap;
        _score = score;
    }
    
    public int overlap() {
        
        return _overlap;
    }
    
    public BigDecimal score() {
        
        return _score;
    }
}
