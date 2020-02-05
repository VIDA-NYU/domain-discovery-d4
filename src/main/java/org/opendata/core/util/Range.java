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

import java.util.ArrayList;
import java.util.List;

/**
 *
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class Range {
    
    public static List<Integer> parseRange(String arg) {
        
        if (arg.equals("-1")) {
            return null;
        } else if (arg.contains("-")) {
            ArrayList<Integer> values = new ArrayList<>();
            int pos = arg.indexOf("-");
	    int start = Integer.parseInt(arg.substring(0, pos));
	    int end = Integer.parseInt(arg.substring(pos + 1));
	    for (int iValue = start; iValue < end; iValue++) {
		values.add(iValue);
	    }
            return values;
        } else {
            ArrayList<Integer> values = new ArrayList<>();
            for (String token : arg.split(",")) {
                values.add(Integer.parseInt(token));
            }
            return values;
        }
    }

}
