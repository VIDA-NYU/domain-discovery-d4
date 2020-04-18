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
package org.opendata.curation.d4.signature.trim;

import org.opendata.curation.d4.signature.SignatureBlocksConsumer;
import org.opendata.core.set.IDSet;
import org.opendata.db.eq.EQIndex;

/**
 * Factory for signature trimmers.
 * 
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class SignatureTrimmerFactory {
    
    private final int[] _nodeSizes;
    private final TrimmerType _trimmerType;
    
    public SignatureTrimmerFactory(int[] nodeSizes, TrimmerType trimmerType) {
        
        _nodeSizes = nodeSizes;
        _trimmerType = trimmerType;
    }
    
    public SignatureTrimmerFactory(EQIndex nodes, TrimmerType trimmerType) {
        
        this(nodes.nodeSizes(), trimmerType);
    }
    
    /**
     * Get column specific trimmer for a given column. We currently do not make
     * use of the empty signature constraint.
     * 
     * @param column
     * @param consumer
     * @return 
     */
    public SignatureTrimmer getTrimmer(IDSet column, SignatureBlocksConsumer consumer) {
        
        switch (_trimmerType) {
            case CONSERVATIVE:
                return new ConservativeTrimmer(column, consumer);
            case CENTRIST:
                return new CentristTrimmer(column, _nodeSizes, consumer);
            default:
                return new NonTrimmer(column, consumer);
        }
    }
}
