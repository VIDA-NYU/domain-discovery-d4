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
package org.opendata.curation.d4.explore;

import java.io.BufferedReader;
import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.opendata.core.io.FileSystem;
import org.opendata.core.set.IdentifiableObjectSet;
import org.opendata.curation.d4.column.ExpandedColumn;
import org.opendata.curation.d4.column.ExpandedColumnReader;
import org.opendata.db.eq.EQ;
import org.opendata.db.eq.EQIndex;
import org.opendata.db.eq.LazyParseEQIndex;

/**
 * Print pairs of terms in a ground-truth domain that do not occur together
 * in any column (but are added to a common column in expansion).
 * 
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class GTDisjointTermsPrinter {
    
    public static void main(String[] args) {
     
        if (args.length != 3) {
            System.out.println("Usage: <eq-file> <expanded-column-file> <gt-file>");
            System.exit(-1);
        }
        
        File eqFile = new File(args[0]);
        File columnsFile = new File(args[1]);
        File gtFile = new File(args[2]);
        
        try (BufferedReader in = FileSystem.openReader(gtFile)) {
            EQIndex eqIndex = new LazyParseEQIndex(eqFile);
            IdentifiableObjectSet<ExpandedColumn> columns;
            columns = new ExpandedColumnReader(columnsFile).read();
            List<EQ> nodes = new ArrayList<>();
            String line;
            while ((line = in.readLine()) != null) {
                String[] tokens = line.split("\t");
                nodes.add(eqIndex.get(Integer.parseInt(tokens[0])));
            }
            for (int iNode = 0; iNode < nodes.size() - 1; iNode++) {
                EQ nodeI = nodes.get(iNode);
                for (int jNode = iNode + 1; jNode < nodes.size(); jNode++) {
                    EQ nodeJ = nodes.get(jNode);
                    if (!nodeI.columns().overlaps(nodeJ.columns())) {
                        for (int columnId : nodeI.columns()) {
                            if (columns.get(columnId).expandedNodes().contains(nodeJ.id())) {
                                System.out.println(
                                        String.format(
                                                "%d -> %d in %d",
                                                nodeI.id(),
                                                nodeJ.id(),
                                                columnId
                                        )
                                );
                            }
                        }
                        for (int columnId : nodeJ.columns()) {
                            if (columns.get(columnId).expandedNodes().contains(nodeI.id())) {
                                System.out.println(
                                        String.format(
                                                "%d -> %d in %d",
                                                nodeJ.id(),
                                                nodeI.id(),
                                                columnId
                                        )
                                );
                            }
                        }
                    }
                }
            }
        } catch (java.io.IOException ex) {
            Logger.getGlobal().log(Level.SEVERE, "RUN", ex);
            System.exit(-1);
        }
    }
}
