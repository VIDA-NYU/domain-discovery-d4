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
package org.opendata.core.object;

import java.math.BigDecimal;
import org.opendata.core.util.FormatedBigDecimal;

/**
 * Identifiable decimal that uses a double data type internally
 * to represent the value.
 * 
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class IdentifiableDouble extends IdentifiableDecimal {
    
    private final double _value;
    
    public IdentifiableDouble(int id, double value) {
        
        super(id);
        
        _value = value;
    }
    
    public IdentifiableDouble(int id, BigDecimal value) {
        
        this(id, value.doubleValue());
    }
    
    @Override
    public BigDecimal asBigDecimal() {
    
        return new BigDecimal(_value);
    }
    
    @Override
    public double asDouble() {
    
        return _value;
    }
    
    @Override
    public int compareTo(IdentifiableDecimal value) {

        return Double.compare(_value, value.asDouble());
    }

	@Override
	public boolean isZero() {

		return _value == 0;
	}
   
    public FormatedBigDecimal toFormatedDecimal() {
    
        return new FormatedBigDecimal(_value);
    }
    
    /**
     * Get double value (for backward compatibility).
     * 
     * @return
     */
    public double value() {
    	
    	return _value;
    }
}
