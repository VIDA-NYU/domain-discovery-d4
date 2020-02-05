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

import org.opendata.core.profiling.datatype.label.DateType;
import java.text.SimpleDateFormat;
import java.util.Date;
import org.opendata.core.profiling.datatype.label.DataType;

/**
 *
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class SimpleDateFormatChecker implements DataTypeChecker {

    private final String _delim;
    private final SimpleDateFormat _df;
    private final int[] _minLength;
    private final String _pattern;
    
    /**
     * Initialize the date format checker using the given pattern string.
     * 
     * @param pattern 
     */
    public SimpleDateFormatChecker(String pattern) {
	
        this(pattern, null, null);
    }
 
    public SimpleDateFormatChecker(String pattern, String delim, int[] minLength) {
	
        _pattern = pattern;
        _delim = delim;
        _minLength = minLength;
        _df = new SimpleDateFormat("'^'" + pattern + "'$'");
        _df.setLenient(false);
    }

    /**
     * Format the given date according to the used simple date format. the date
     * may be null. It that case the result is an empty string.
     * 
     * @param date
     * @return 
     */
    public String format(Date date) {
	
        if (date != null) {
            return new SimpleDateFormat(_pattern).format(date);
        } else {
            return "";
        }
    }
    
    @Override
    public boolean isMatch(String value) {

        try {
            _df.parse("^" + value + "$");
            if (_delim != null) {
                String[] tokens = value.split(_delim);
                if (tokens.length <= _minLength.length) {
                    for (int iToken = 0; iToken < _minLength.length; iToken++) {
                        if (tokens[iToken].length() < _minLength[iToken]) {
                            return false;
                        }
                    }
                } else {
                    return false;
                }
            }
            return true;
        } catch (java.text.ParseException ex) {
            return false;
        }
    }

    @Override
    public DataType label() {

        return new DateType();
    }
    
    /**
     * String representation of the checked pattern.
     * 
     * @return 
     */
    public String toPattern() {
	
        return _pattern;
    }
}
