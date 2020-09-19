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

import org.opendata.core.constraint.Threshold;
import org.opendata.curation.d4.signature.SignatureBlocksConsumer;
import org.opendata.core.set.IDSet;
import org.opendata.db.eq.EQIndex;

/**
 * Factory for signature trimmers.
 * 
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class SignatureTrimmerFactory {
    
    private final EQIndex _nodes;
    private final String _trimmerSpec;
    
    public SignatureTrimmerFactory(EQIndex nodes, String trimmerSpec) {
        
        _nodes = nodes;
        _trimmerSpec = trimmerSpec;
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
        
        if (_trimmerSpec.equals(SignatureTrimmer.CONSERVATIVE)) {
            return new ConservativeTrimmer(column, consumer);
        } else if (_trimmerSpec.equals(SignatureTrimmer.CENTRIST)) {
            return new CentristTrimmer(column, _nodes.nodeSizes(), consumer);
        } else if (_trimmerSpec.startsWith(SignatureTrimmer.CENTRIST)) {
            int pos = _trimmerSpec.indexOf(":");
            if (pos != -1) {
                return new CentristTrimmer(
                        column,
                        _nodes.nodeSizes(),
                        Threshold.getConstraint(_trimmerSpec.substring(pos + 1)),
                        consumer
                );                
            }
        } else if (_trimmerSpec.equals(SignatureTrimmer.LIBERAL)) {
            return new NonTrimmer(column, consumer);
        }
        throw new IllegalArgumentException(String.format("Invalid trimmer: %s", _trimmerSpec));
    }
}
