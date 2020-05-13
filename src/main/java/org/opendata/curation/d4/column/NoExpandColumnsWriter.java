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
package org.opendata.curation.d4.column;

import java.io.File;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.opendata.curation.d4.Constants;
import org.opendata.curation.d4.signature.SignatureBlocksGenerator;
import org.opendata.core.set.IdentifiableObjectSet;
import org.opendata.db.Database;
import org.opendata.db.column.Column;
import org.opendata.db.eq.EQIndex;

/**
 * Write file for all columns in a database without expansion information.
 * 
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class NoExpandColumnsWriter {
    
    public void run(
            EQIndex nodes,
            ExpandedColumnConsumer consumer
    ) {
        
        consumer.open();
        
        for (Column column : new Database(nodes).columns()) {
            consumer.consume(new ImmutableExpandedColumn(column));
        }
        
        consumer.close();
    }
    
    private static final String COMMAND =
            "Usage\n" +
            "  <eq-file>\n" +
            "  <output-file>";
    
    private static final Logger LOGGER = Logger
            .getLogger(SignatureBlocksGenerator.class.getName());
    
    public static void main(String[] args) {
        
        System.out.println(Constants.NAME + " - Column No-Expander - Version (" + Constants.VERSION + ")\n");

        if (args.length != 2) {
            System.out.println(COMMAND);
            System.exit(-1);
        }
        
        File eqFile = new File(args[0]);
        File outputFile = new File(args[1]);

        try {
            // Read the node index and the list of columns
            EQIndex nodeIndex = new EQIndex(eqFile);
            IdentifiableObjectSet<Column> db = nodeIndex.columns();
            new NoExpandColumnsWriter().run(
                    nodeIndex,
                    new ExpandedColumnWriter(outputFile)
            );
        } catch (java.io.IOException ex) {
            LOGGER.log(Level.SEVERE, "RUN", ex);
            System.exit(-1);
        }
    }
}