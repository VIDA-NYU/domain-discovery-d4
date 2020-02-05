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
package org.opendata.core.util;

import java.math.BigDecimal;
import java.math.RoundingMode;

/**
 * Create average for a stream of values.
 * 
 * Maintains total count and element count.
 * 
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class AverageGenerator {
   
    private int _count = 0;
    private long _sum = 0;
    
    public void add(int value) {
        
        _sum += (long)value;
        _count++;
    }
    
    public int size() {
        
        return _count;
    }
    
    public long sum() {
        
        return _sum;
    }
    
    public String toPrintString() {
        
        BigDecimal avg = new BigDecimal(this.value())
                .setScale(6, RoundingMode.HALF_DOWN);
        return "COUNT=" + _count + ", SUM=" + _sum + ", AVG=" + avg.toPlainString();
    }
    
    public double value() {
        
        if (_count > 0) {
            return (double)_sum/(double)_count;
        } else {
            return 0;
        }
    }
}
