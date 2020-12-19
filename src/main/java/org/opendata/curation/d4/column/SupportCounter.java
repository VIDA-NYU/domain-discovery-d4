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
package org.opendata.curation.d4.column;

import java.math.BigDecimal;
import java.math.MathContext;

/**
 * Helper class to maintain support information for equivalence classes in
 * column expansion.
 * 
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class SupportCounter {
    
    private int _expansionSupport = 0;
    private int _originalSupport = 0;

    public int expansionSupportCount() {

        return _expansionSupport;
    }

    public void incExpansionSupport(int value) {

        _expansionSupport += value;
    }

    public void incOriginalSupport(int value) {

        _originalSupport += value;
    }

    public BigDecimal originalSupport(int size) {

        if (size == 0) {
            return BigDecimal.ZERO;
        }

        return new BigDecimal(_originalSupport)
                .divide(new BigDecimal(size), MathContext.DECIMAL64);
    }

    public int originalSupportCount() {

        return _originalSupport;
    }

    public BigDecimal overallSupport(int size) {

        if (size == 0) {
            return BigDecimal.ZERO;
        }

        return new BigDecimal(_originalSupport + _expansionSupport)
                .divide(new BigDecimal(size), MathContext.DECIMAL64);
    }

    public int overallSupportCount() {

        return _originalSupport + _expansionSupport;
    }
}
