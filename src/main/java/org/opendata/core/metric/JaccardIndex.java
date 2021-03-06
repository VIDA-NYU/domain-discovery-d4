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
package org.opendata.core.metric;

import java.math.BigDecimal;
import java.math.MathContext;

/**
 *
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class JaccardIndex {

    private final static MathContext MC = MathContext.DECIMAL64;
	
    public BigDecimal logSim(int size1, int size2, int overlap) {

    	int divisor = (size1 + size2) - overlap;
    	if (overlap == divisor) {
    		return BigDecimal.ONE;
    	}
        return new BigDecimal(Math.log(overlap + 1))
        		.divide(new BigDecimal(Math.log(divisor + 1)), MC);
    }

    public BigDecimal sim(int size1, int size2, int overlap) {

    	int divisor = (size1 + size2) - overlap;
    	if (overlap == divisor) {
            return BigDecimal.ONE;
    	}
        return new BigDecimal(overlap).divide(new BigDecimal(divisor), MC);
    }
}
