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
package org.opendata.db.column;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.opendata.core.io.EntitySetReader;
import org.opendata.core.object.Entity;
import org.opendata.core.set.HashIDSet;
import org.opendata.db.Database;
import org.opendata.db.eq.EQIndex;

/**
 * Print set of terms in a column.
 * 
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class ColumnPrinter {
    
    /**
     * Get sorted list of terms in the column.
     * 
     * @param eqIndex
     * @param reader
     * @param columnId
     * @return 
     * @throws java.io.IOException 
     */
    public List<String> read(EQIndex eqIndex, EntitySetReader reader, int columnId) throws java.io.IOException {
        
        Database db = new Database(eqIndex);
        
        HashIDSet termFilter = new HashIDSet();
        for (int nodeId : db.columns().get(columnId)) {
            termFilter.add(eqIndex.get(nodeId).terms());
        }
        
        List<String> terms = new ArrayList<>();
        for (Entity entity : reader.readEntities(termFilter)) {
            terms.add(entity.name());
        }
        Collections.sort(terms);
        return terms;
    }
    
    private static final String COMMAND =
            "Usage:\n" +
            "  <eq-file>\n" +
            "  <term-file>\n" +
            "  <column-id>";
    
    private static final Logger LOGGER = Logger
            .getLogger(ColumnPrinter.class.getName());
    
    public static void main(String[] args) {
        
        if (args.length != 3) {
            System.out.println(COMMAND);
            System.exit(-1);
        }
        
        File eqFile = new File(args[0]);
        File termFile = new File(args[1]);
        int columnId = Integer.parseInt(args[2]);
        
        try {
            List<String> terms;
            terms = new ColumnPrinter()
                    .read(
                            new EQIndex(eqFile),
                            new EntitySetReader(termFile),
                            columnId
                    );
            for (String term : terms) {
                System.out.println(term);
            }
        } catch (java.io.IOException ex) {
            LOGGER.log(Level.SEVERE, "RUN", ex);
            System.exit(-1);
        }
    }
}
