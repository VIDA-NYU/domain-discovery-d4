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
import java.math.MathContext;
import org.opendata.core.set.IDSet;
import org.opendata.curation.d4.signature.SignatureBlock;

/**
 * Block score function that uses the smaller of the column size and block size
 * as the divisor.
 * 
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class MinJIScore extends BlockScoreFunction {

    private final int _columnSize;
    
    public MinJIScore(IDSet column, Integer[] eqTermCounts) {
        
        super(column, eqTermCounts);
        
        int termCount = 0;
        for (int eqId : column) {
            termCount += eqTermCounts[eqId];
        }
        _columnSize = termCount;
    }
    
    @Override
    public BigDecimal score(SignatureBlock block) {

        int overlap = this.overlap(block);
        if (overlap > 0) {
            return new BigDecimal(overlap)
                    .divide(
			    new BigDecimal(Math.min(_columnSize, block.termCount())),
			    MathContext.DECIMAL64
		    );
        } else {
            return BigDecimal.ZERO;
        }
    }
}
