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
package org.opendata.curation.d4.export;

import com.google.gson.stream.JsonWriter;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import org.opendata.core.io.FileSystem;
import org.opendata.core.set.HashIDSet;
import org.opendata.core.set.IdentifiableObjectSet;
import org.opendata.core.sort.EntityNameSort;
import org.opendata.curation.d4.domain.Domain;
import org.opendata.db.eq.EQIndex;
import org.opendata.db.term.Term;
import org.opendata.db.term.TermIndexReader;

/**
 * Export domains as JSON files. Each domain will be written into a separate
 * JSON file. The file contains the list of columns that the domain occurs in
 * and the list of terms in the domain.
 * 
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class DomainsExportWriter {
    
    public void run(
            EQIndex eqIndex,
            TermIndexReader reader,
            List<Domain> domains,
            File columnsFile,
            File outputDir
    ) throws java.io.IOException {
        
        // Read column metadata
        HashMap<Integer, String[]> columns = new HashMap<>();
        try (BufferedReader in = FileSystem.openReader(columnsFile)) {
            String line;
            while ((line = in.readLine()) != null) {
                String[] tokens = line.split("\t");
                int columnId = Integer.parseInt(tokens[0]);
                columns.put(columnId, new String[]{tokens[1], tokens[2]});
            }
        }
        
        // Create the output directory if it does not exist.
        FileSystem.createFolder(outputDir);
        
        // Collect the identifier of all terms in the given domain list
        HashIDSet terms = new HashIDSet();
        for (Domain domain : domains) {
            for (int nodeId : domain) {
                for (int termId : eqIndex.get(nodeId).terms()) {
                    terms.add(termId);
                }
            }
        }
        
        // Read terms from term index
        IdentifiableObjectSet<Term> termIndex = reader.read(terms);
        
        // Write each domain to a separate file in the output directory
        for (Domain domain : domains) {
            // Create sorted list of terms
            ArrayList<Term> domainTerms = new ArrayList<>();
            for (int nodeId : domain) {
                for (int termId : eqIndex.get(nodeId).terms()) {
                    domainTerms.add(termIndex.get(termId));
                }
            }
            Collections.sort(domainTerms, new EntityNameSort());
            // Write domain columns and terms to file. The output file is named
            // by the domain identifier.
            File outputFile = FileSystem.joinPath(outputDir, domain.id() + ".json");
            try (JsonWriter writer = new JsonWriter(new FileWriter(outputFile))) {
                writer.beginObject();
                writer.name("columns");
                writer.beginArray();
                for (int columnId : domain.columns()) {
                    String[] column = columns.get(columnId);
                    writer.beginObject();
                    writer.name("name").value(column[0]);
                    writer.name("dataset").value(column[1]);
                    writer.endObject();
                }
                writer.endArray();
                writer.name("terms");
                writer.beginArray();
                for (Term term : domainTerms) {
                    writer.value(term.name());
                }
                writer.endArray();
                writer.endObject();
            }
        }
    }
}
