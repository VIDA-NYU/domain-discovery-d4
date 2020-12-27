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

import java.math.BigDecimal;
import org.opendata.core.set.IDSet;
import org.opendata.curation.d4.signature.SignatureBlock;

/**
 * Score function for signature blocks.
 * 
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public abstract class BlockScoreFunction {
    
    private final Integer[] _column;
    private final Integer[] _eqTermCounts;
    
    public BlockScoreFunction(IDSet column, Integer[] eqTermCounts) {
        
        _column = new Integer[column.length()];
        column.toSortedList().toArray(_column);
        _eqTermCounts = eqTermCounts;
    }
    
    public int overlap(SignatureBlock block) {

        final int len1 = _column.length;
        final int len2 = block.elementCount();
        
        int idx1 = 0;
        int idx2 = 0;
        int overlap = 0;
        
        while ((idx1 < len1) && (idx2 < len2)) {
            int comp = Integer.compare(_column[idx1], block.elementAt(idx2));
            if (comp < 0) {
                idx1++;
            } else if (comp > 0) {
                idx2++;
            } else {
                overlap += _eqTermCounts[_column[idx1]];
                idx1++;
                idx2++;
            }
        }
        
        return overlap;
    }
    /**
     * Get score for a given signature block.
     * 
     * @param block
     * @return 
     */
    public abstract BigDecimal score(SignatureBlock block);
}
