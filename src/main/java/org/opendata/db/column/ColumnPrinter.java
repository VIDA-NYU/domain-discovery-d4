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
import java.util.logging.Level;
import java.util.logging.Logger;
import org.opendata.core.io.EntitySetReader;
import org.opendata.core.set.EntitySet;
import org.opendata.core.set.IDSet;
import org.opendata.db.eq.EQIndex;

/**
 * Print nodes and terms in a database column.
 * 
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class ColumnPrinter {
   
    private final static String COMMAND =
            "Usage:\n" +
            "  <eq-file>\n" +
            "  <term-file>\n" +
            "  <column-id>";
    
    private final static Logger LOGGER = Logger
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
            EQIndex eqIndex = new EQIndex(eqFile);
            IDSet column = eqIndex.columns().get(columnId);
            EntitySet terms = new EntitySetReader(termFile).readEntities(eqIndex, column);
            for (int nodeId : column) {
                int[] termIds = eqIndex.get(nodeId).terms().toArray();
                System.out.println(nodeId + "\t" + terms.get(termIds[0]).name());
                for (int i = 1; i < termIds.length; i++) {
                    System.out.println("\t" + terms.get(termIds[i]).name());
                }
            }
        } catch (java.io.IOException ex) {
            LOGGER.log(Level.SEVERE, "RUN", ex);
        }
    }
}
