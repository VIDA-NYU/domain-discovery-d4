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
package org.opendata.profiling.datatype;

import org.opendata.core.object.Entity;

/**
 * Abstract of labels that are used to classify data types of values and
 * columns in a dataset.
 * 
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public abstract class DataType extends Entity {
    
    public static final int DATE = 10;
    public static final int DECIMAL = 20;
    public static final int GEO = 30;
    public static final int INTEGER = 40;
    public static final int LONG = 50;
    public static final int TEXT = 60;
    
    public DataType(int identifier, String name) {
        
        super(identifier, name);
    }
    
    /**
     * True, if the data type label is DATE.
     * 
     * @return 
     */
    public boolean isDate() {
        
        return (this.id() == DATE);
    }
    
    /**
     * True, if the data type is used to specify spatial locations.
     * 
     * @return 
     */
    public boolean isGeoLocation() {
        
        return (this.id() == GEO);
    }
    
    /**
     * True, if the label is one of the numeric types.
     * 
     * @return 
     */
    public boolean isNumeric() {
        
        switch (this.id()) {
            case DECIMAL:
            case INTEGER:
            case LONG:
                return true;
        }
        return false;
    }
    
    /**
     * true, if label is TEXT.
     * 
     * @return 
     */
    public boolean isText() {
        
        return (this.id() == TEXT);
    }
}
