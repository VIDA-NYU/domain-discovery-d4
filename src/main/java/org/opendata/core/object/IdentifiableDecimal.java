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
 * Identifiable object that contains a decimal value. Different implementations
 * may use different data types (precision) to represent the decimal value. Mainly
 * for purpose of reducing the amount of memory that is being consumed.
 * 
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 *
 */
public abstract class IdentifiableDecimal extends IdentifiableObjectImpl implements Comparable<IdentifiableDecimal> {

	/**
	 * Initialize the unique object identifier.
	 * 
	 * @param id
	 */
	public IdentifiableDecimal(int id) {
		
		super(id);
	}
	
	/**
	 * Get internal value as a BigDecimal.
	 * 
	 * @return
	 */
	public abstract BigDecimal asBigDecimal();
	
	/**
	 * Get internal value as a double value.
	 * 
	 * @return
	 */
	public abstract double asDouble();
	
	
	/**
	 * Test if the decimal value is zero.
	 * 
	 * @return
	 */
	public abstract boolean isZero();
	
	/**
	 * Get formated decimal for printing.
	 * 
	 * @return
	 */
	public abstract FormatedBigDecimal toFormatedDecimal();
	
    /**
     * Get plain string representation of the decimal value.
     * 
     * @return
     */
    public String toPlainString() {
    
        return this.toFormatedDecimal().toString();
    }

    @Override
	public String toString() {
		
		return String.format(
				"<%d, %s>",
				this.id(),
				this.toPlainString()
		);
	}
}
