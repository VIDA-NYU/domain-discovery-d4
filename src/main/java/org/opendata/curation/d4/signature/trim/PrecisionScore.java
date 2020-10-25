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
import org.opendata.core.metric.Precision;
import org.opendata.core.set.IdentifiableObjectSet;
import org.opendata.db.column.Column;
import org.opendata.db.eq.EQIndex;

/**
 *
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class PrecisionScore extends BlockScoreFunction {

    public PrecisionScore(EQIndex eqIndex, IdentifiableObjectSet<Column> columns) {
        
        super(eqIndex, columns);
    }

    public PrecisionScore(EQIndex eqIndex) {
        
        super(eqIndex, eqIndex.columns());
    }
    
    @Override
    public BigDecimal relevance(int columnSize, int blockSize, int overlap) {

        return new Precision(overlap, blockSize).value();
    }    
}
