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
package org.opendata.core.profiling.datatype;

import java.math.BigDecimal;
import org.opendata.core.profiling.datatype.label.DataType;
import org.opendata.core.profiling.datatype.label.DecimalType;

/**
 * Slightly advanced implementation for a decimal checker. Tries to create a
 * BigDecimal from a given string after removing all ','.
 * 
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class AdvancedDecimalChecker implements DataTypeChecker {

    @Override
    public boolean isMatch(String value) {
	
        try {
            new BigDecimal(value.replaceAll(",", ""));
            return true;
        } catch (java.lang.NumberFormatException ex) {
            return false;
        }
    }

    @Override
    public DataType label() {

        return new DecimalType();
    }
}
