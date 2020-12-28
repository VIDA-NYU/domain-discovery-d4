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
package org.opendata.curation.d4;

import org.opendata.core.set.IDSet;
import org.opendata.curation.d4.column.ExpandedColumn;
import org.opendata.curation.d4.signature.RobustSignatureConsumer;
import org.opendata.curation.d4.signature.trim.CentristTrimmer;
import org.opendata.curation.d4.signature.trim.ConservativeTrimmer;
import org.opendata.curation.d4.signature.trim.NonTrimmer;
import org.opendata.curation.d4.signature.trim.PrecisionScore;
import org.opendata.curation.d4.signature.trim.SignatureTrimmer;

/**
 * Factory for signature trimmers.
 * 
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class SignatureTrimmerFactory {
    
    private final Integer[] _eqTermCounts;
    private final String _identifier;
    private final boolean _originalOnly;
    
    public SignatureTrimmerFactory(
            String identifier,
            Integer[] eqTermCounts,
            boolean originalOnly
    ) {
        
        _identifier = identifier;
        _eqTermCounts = eqTermCounts;
        _originalOnly = originalOnly;
    }
    
    /**
     * Get column specific trimmer for a given column. We currently do not make
     * use of the empty signature constraint.
     * 
     * @param column
     * @param consumer
     * @return 
     */
    public SignatureTrimmer getSignatureTrimmer(IDSet column, RobustSignatureConsumer consumer) {
        
        if (_identifier.equals(D4Config.TRIMMER_CONSERVATIVE)) {
            return new ConservativeTrimmer(column, consumer);
        } else if (_identifier.equals(D4Config.TRIMMER_CENTRIST)) {
            return new CentristTrimmer(column, new PrecisionScore(column, _eqTermCounts), consumer);
        } else if (_identifier.equals(D4Config.TRIMMER_LIBERAL)) {
            return new NonTrimmer(column, consumer);
        }
        throw new IllegalArgumentException(
                String.format("Invalid trimmer: %s", _identifier)
        );
    }
    
    public SignatureTrimmer getSignatureTrimmer(ExpandedColumn column, RobustSignatureConsumer consumer) {
        
        if (_originalOnly) {
            return this.getSignatureTrimmer(column.originalNodes(), consumer);
        } else {
            return this.getSignatureTrimmer(column.nodes(), consumer);
        }
    }
}
