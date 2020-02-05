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

import org.opendata.core.profiling.datatype.label.DataType;
import org.opendata.core.profiling.datatype.label.GeoType;

/**
 * Type checker for geo-spatial locations.
 * 
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class GeoPointChecker implements DataTypeChecker {

    private boolean isDouble(String value) {
        
        try {
            Double.parseDouble(value);
            return true;
        } catch (java.lang.NumberFormatException ex) {
            return false;
        }
    }
    
    @Override
    public boolean isMatch(String value) {

        if ((value.startsWith("(")) && (value.endsWith(")"))) {
            int pos = value.indexOf(",");
            if (pos != -1) {
                String val = value.substring(1, pos);
                if (this.isDouble(val)) {
                    val = value.substring(pos + 1, value.length() - 1);
                    return this.isDouble(val);
                }
            }
        }
        return false;
    }    

    @Override
    public DataType label() {

        return new GeoType();
    }
}
