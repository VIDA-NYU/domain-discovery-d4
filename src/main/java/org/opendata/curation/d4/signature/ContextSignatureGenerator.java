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

import java.util.ArrayList;
import java.util.List;
import org.opendata.db.eq.similarity.EQSimilarity;

/**
 * Generate context signatures for equivalence classes (column elements).
 * 
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class ContextSignatureGenerator {

    private final List<Integer> _equivalenceClasses;
    private final EQSimilarity _simFunc;
    
    public ContextSignatureGenerator(List<Integer> equivalenceClasses, EQSimilarity simFunc) {
        
        _equivalenceClasses = equivalenceClasses;
        _simFunc = simFunc;
    }
    
    /**
     * Compute signature for element with given identifier.
     * 
     * @param id
     * @return 
     */
    public ContextSignature getSignature(int id) {

        
        ArrayList<SignatureValue> elements = new ArrayList<>();        
        for (Integer eqJ : _equivalenceClasses) {
            if (id != eqJ) {
                elements.add(new SignatureValue(eqJ, _simFunc.sim(id, eqJ)));
            }
        }
        
        return new ContextSignature(id, elements);
    }  
}
