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
package org.opendata.core.value;

/**
 * Filter column values based on value length and the token count thresholds.
 *
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class LengthAndTokenCountFilter implements ValueFilter {

    private final int _maxValueLength;
    private final int _maxTokenCount;
    
    public LengthAndTokenCountFilter(int maxValueLength, int maxTokenCount) {
        
        _maxValueLength = maxValueLength;
        _maxTokenCount = maxTokenCount;
    }
    
    @Override
    public boolean accept(String value) {

        if (value.length() == 0) {
            return false;
        } else if ((_maxValueLength > 0) && (value.length() > _maxValueLength)) {
            return false;
        } else {
            if ((_maxTokenCount > 0) && (value.split("\\s").length > _maxTokenCount)) {
                return false;
            }
        }
        return true;
    }
}
