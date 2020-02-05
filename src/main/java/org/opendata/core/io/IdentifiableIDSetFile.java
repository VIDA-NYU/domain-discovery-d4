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
package org.opendata.core.io;

import java.io.PrintWriter;
import org.opendata.core.set.IdentifiableIDSet;
import org.opendata.core.set.ImmutableIDSet;
import org.opendata.core.set.ImmutableIdentifiableIDSet;

/**
 * Reader and writer for identifiable ID sets that are maintained as single
 * line strings.
 * 
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class IdentifiableIDSetFile {
   
    public static final int DEFAULT_LIST_COLUMN_INDEX = 1;
    
    public IdentifiableIDSet parse(String line, int listColumnIndex) {
        
        String[] tokens = line.split("\t");
        int nodeId = Integer.parseInt(tokens[0]);
        if (tokens.length == 1) {
            return new ImmutableIdentifiableIDSet(nodeId);
        } else {
            return new ImmutableIdentifiableIDSet(
                    nodeId,
                    new ImmutableIDSet(tokens[listColumnIndex])
            );
        }
    }
    
    public void write(IdentifiableIDSet set, PrintWriter out) {

        if (!set.isEmpty()) {
            out.println(set.id() + "\t" + set.toIntString());
        } else {
            out.println(set.id());
        }
    }
}
