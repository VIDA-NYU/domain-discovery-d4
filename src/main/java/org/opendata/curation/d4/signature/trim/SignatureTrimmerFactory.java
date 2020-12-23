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

import java.util.HashMap;
import org.opendata.core.set.HashIDSet;
import org.opendata.core.set.SortedIDList;
import org.opendata.curation.d4.signature.RobustSignatureConsumer;

/**
 * Factory for signature trimmers.
 * 
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class SignatureTrimmerFactory {
    
    private final HashMap<Integer, SortedIDList> _columns;
    private final Integer[] _eqTermCounts;
    private PrecisionScore _scoreFunc = null;
    private final String _trimmerSpec;
    
    public SignatureTrimmerFactory(
            HashMap<Integer, SortedIDList> columns,
            Integer[] eqTermCounts,
            String trimmerSpec
    ) {
        _columns = columns;
        _eqTermCounts = eqTermCounts;
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
        
        HashIDSet filter = new HashIDSet(_columns.get(columnId));
        if (_trimmerSpec.equals(SignatureTrimmer.CONSERVATIVE)) {
            return new ConservativeTrimmer(filter, consumer);
        } else if (_trimmerSpec.equals(SignatureTrimmer.CENTRIST)) {
            if (_scoreFunc == null) {
                _scoreFunc = new PrecisionScore(_columns, _eqTermCounts);
            }
            return new CentristTrimmer(columnId, filter, _scoreFunc, consumer);
        } else if (_trimmerSpec.equals(SignatureTrimmer.LIBERAL)) {
            return new NonTrimmer(filter, consumer);
        }
        throw new IllegalArgumentException(String.format("Invalid trimmer: %s", _trimmerSpec));
    }
}
