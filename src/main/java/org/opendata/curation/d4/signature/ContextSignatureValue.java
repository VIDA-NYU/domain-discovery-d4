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
package org.opendata.curation.d4.signature;

import java.math.BigDecimal;
import org.opendata.core.object.IdentifiableDouble;
import org.opendata.db.eq.similarity.SimilarityScore;

/**
 * Element in a context signature. Maintains the overlap between the column sets
 * of two equivalence classes and their similarity.
 * 
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class ContextSignatureValue extends IdentifiableDouble {
    
    private final int _overlap;
    
    public ContextSignatureValue(int id, int overlap, double value) {

        super(id, value);
        
        _overlap = overlap;
    }
    
    public ContextSignatureValue(int id, int overlap, BigDecimal value) {

        this(id, overlap, value.doubleValue());
    }
    
    public ContextSignatureValue(int id, SimilarityScore sim) {
    
        this(id, sim.overlap(), sim.score());
    }
    
    public int overlap() {
        
        return _overlap;
    }
}
