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

import org.opendata.core.set.IdentifiableIDSet;
import org.opendata.core.set.IdentifiableObjectSet;
import org.opendata.curation.d4.signature.RobustSignatureConsumer;
import org.opendata.db.column.Column;

/**
 * Factory for signature trimmers.
 * 
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class SignatureTrimmerFactory {
    
    private final IdentifiableObjectSet<Column> _columns;
    private final Integer[] _eqTermCounts;
    private PrecisionScore _scoreFunc = null;
    private final String _trimmerSpec;
    
    public SignatureTrimmerFactory(
            Integer[] eqTermCounts,
            IdentifiableObjectSet<Column> columns,
            String trimmerSpec
    ) {
        _eqTermCounts = eqTermCounts;
        _columns = columns;
        _trimmerSpec = trimmerSpec;
    }
    
    /**
     * Get column specific trimmer for a given column. We currently do not make
     * use of the empty signature constraint.
     * 
     * @param columnId
     * @param consumer
     * @return 
     */
    public SignatureTrimmer getTrimmer(int columnId, RobustSignatureConsumer consumer) {
        
        IdentifiableIDSet column = _columns.get(columnId);
        if (_trimmerSpec.equals(SignatureTrimmer.CONSERVATIVE)) {
            return new ConservativeTrimmer(column, consumer);
        } else if (_trimmerSpec.equals(SignatureTrimmer.CENTRIST)) {
            if (_scoreFunc == null) {
                _scoreFunc = new PrecisionScore(_eqTermCounts, _columns);
            }
            return new CentristTrimmer(column, _scoreFunc, consumer);
        } else if (_trimmerSpec.equals(SignatureTrimmer.LIBERAL)) {
            return new NonTrimmer(column, consumer);
        }
        throw new IllegalArgumentException(String.format("Invalid trimmer: %s", _trimmerSpec));
    }
}
