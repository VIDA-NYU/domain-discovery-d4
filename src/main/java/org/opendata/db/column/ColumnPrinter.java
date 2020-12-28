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
import org.opendata.core.object.Entity;
import org.opendata.db.Database;
import org.opendata.db.SortedEntitySet;
import org.opendata.db.eq.CompressedTermIndexFile;
import org.opendata.db.term.TermIndexReader;

/**
 * Print list of terms in a database columns. Outputs all terms (or only sample
 * for larger equivalence classes if selected) together with the identifier of
 * the equivalence class they belong to. Terms are sorted in alphabetic order.
 * 
 * @author @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class ColumnPrinter {
    
    private final static String COMMAND =
            "Usage:\n" +
            "  <eq-file>\n" +
            "  <term-file>\n" +
            "  <column-id>\n" +
            "  {<sample-size>}";
    
    public static void main(String[] args) {
        
        if ((args.length < 3) || (args.length > 4)) {
            System.out.println(COMMAND);
            System.exit(-1);
        }
        
        File eqFile = new File(args[0]);
        File termFile = new File(args[1]);
        int columnIndex = Integer.parseInt(args[2]);
        
        int sampleSize;
        if (args.length == 4) {
            sampleSize = Integer.parseInt(args[3]);
        } else {
            sampleSize = Integer.MAX_VALUE;
        }
        
        Database db;
        db = new Database(new CompressedTermIndexFile(eqFile), new TermIndexReader(termFile));
        
        for (Entity term : new SortedEntitySet(db.read(columnIndex, sampleSize))) {
            System.out.println(String.format("%d\t%s", term.id(), term.name()));
        }
    }
}
