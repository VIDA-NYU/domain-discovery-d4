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

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;
import java.io.BufferedReader;
import java.io.File;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.opendata.core.io.FileSystem;
import org.opendata.core.object.IdentifiableDouble;
import org.opendata.core.prune.MaxDropFinder;
import org.opendata.core.set.HashIDSet;
import org.opendata.core.set.IdentifiableObjectSet;
import org.opendata.core.sort.DoubleValueDescSort;
import org.opendata.core.util.StringHelper;
import org.opendata.core.util.count.Counter;
import org.opendata.curation.d4.Constants;
import org.opendata.curation.d4.domain.StrongDomain;
import org.opendata.curation.d4.domain.StrongDomainMember;
import org.opendata.curation.d4.domain.StrongDomainReader;
import org.opendata.db.eq.EQIndex;
import org.opendata.db.term.Term;
import org.opendata.db.term.TermIndexReader;

/**
 * Write terms in a strong domain to file. Outputs a tab-delimited text line
 * for each term with the following components:
 * 
 * - term
 * - size of the support set for the strong domain
 * - comma-separated list of local domains (from the support set) that the term
 *   occurs in
 * 
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class ExportStrongDomains {
    
    private List<List<IdentifiableDouble>> getBlocks(List<IdentifiableDouble> items) {
        
        MaxDropFinder<IdentifiableDouble> dropFinder = new MaxDropFinder<>(0.0, true, true);

        int start = 0;
        final int end = items.size();
        ArrayList<List<IdentifiableDouble>> blocks = new ArrayList<>();
        while (start < end) {
            int pruneIndex = dropFinder.getPruneIndex(items, start);
            if (pruneIndex <= start) {
                break;
            }
            List<IdentifiableDouble> block = new ArrayList<>();
            for (int iEl = start; iEl < pruneIndex; iEl++) {
                block.add(items.get(iEl));
            }
            blocks.add(block);
            start = pruneIndex;
        }
        return blocks;
    }

    private String getDomainName(List<String> names) {
    
        HashMap<String, Counter> tokens = new HashMap<>();
        for (String name : names) {
            for (String token : name.split("[\\s_]")) {
                token = token.toLowerCase();
                if ((!tokens.containsKey(token)) && (!token.isEmpty())) {
                    tokens.put(token, new Counter(0));
                }
            }
        }
        
        int maxCount = 0;
        for (String name : names) {
            name = name.toLowerCase();
            for (String token : tokens.keySet()) {
                if (name.contains(token)) {
                    int count = tokens.get(token).inc();
                    if (count > maxCount) {
                        maxCount = count;
                    }
                }
            }
        }
        
        List<String> candidates = new ArrayList<>();
        for (String token : tokens.keySet()) {
            if (tokens.get(token).value() == maxCount) {
                candidates.add(token);
            }
        }
        
        if (candidates.size() > 1) {
            Collections.sort(candidates);
            return StringHelper.joinStrings(candidates);
        } else {
            return candidates.get(0);
        }
    }
    
    public void run(
            File eqFile,
            File termFile,
            File columnFile,
            File strongDomainFile,
            int sampleSize,
            File outputDir
    ) throws java.io.IOException {
        
        // Create the output directory if it does not exist. Remove all files
        // from the directory if it exists.
        FileSystem.createFolder(outputDir);
        for (File file : outputDir.listFiles()) {
            file.delete();
        }
        
        // Read column names. Expects a tab-delimited file with column id,
        // dataset-id and column name.
        HashMap<Integer, String[]> columnNames = new HashMap<>();
        if (columnFile.exists()) {
            try (BufferedReader in = FileSystem.openReader(columnFile)) {
                String line;
                while ((line = in.readLine()) != null) {
                    String[] tokens = line.split("\t");
                    int columnId = Integer.parseInt(tokens[0]);
                    String columnName = tokens[1];
                    String dataset = tokens[2];
                    columnNames.put(columnId, new String[]{columnName, dataset});
                }
            }
        } else {
            LOGGER.log(
                    Level.WARNING,
                    String.format(
                            "File %s does not exist",
                            columnFile.getAbsolutePath()
                    )
            );
        }
        // Read the strong domains
        IdentifiableObjectSet<StrongDomain> strongDomains;
        strongDomains = new StrongDomainReader(strongDomainFile).read();
        
        // Collect term identifier for all equivalence classes in the 
        // strong domains.
        EQIndex eqIndex = new EQIndex(eqFile);
        
        HashIDSet termFilter = new HashIDSet();
        for (StrongDomain domain : strongDomains) {
            for (StrongDomainMember node : domain.members()) {
                termFilter.add(eqIndex.get(node.id()).terms().sample(sampleSize));
            }
        }
        
        // Read information about all terms
        IdentifiableObjectSet<Term> terms;
        terms = new TermIndexReader(termFile).read(termFilter);
        
        // Write each domain to a separate file in the output directory
        Gson gson = new GsonBuilder().setPrettyPrinting().create();
        for (StrongDomain domain : strongDomains) {
            String filename = domain.id() + ".json";
            File outputFile = FileSystem.joinPath(outputDir, filename);
            try (PrintWriter out = FileSystem.openPrintWriter(outputFile)) {
                List<String> names = new ArrayList<>();
                JsonArray arrColumns = new JsonArray();
                for (int columnId : domain.columns()) {
                    String[] columnInfo;
                    if (columnNames.containsKey(columnId)) {
                        columnInfo = columnNames.get(columnId);
                    } else {
                        columnInfo = new String[]{"unknown", "unknown"};
                    }
                    JsonObject objCol = new JsonObject();
                    objCol.add("id", new JsonPrimitive(columnId));
                    objCol.add("name", new JsonPrimitive(columnInfo[0]));
                    objCol.add("dataset", new JsonPrimitive(columnInfo[1]));
                    arrColumns.add(objCol);
                    names.add(columnInfo[0]);
                }
                String domainName = this.getDomainName(names);
                List<IdentifiableDouble> items = new ArrayList<>();
                for (StrongDomainMember node : domain.members()) {
                    double weight = node.weight().doubleValue();
                    for (int termId : eqIndex.get(node.id()).terms()) {
                        items.add(new IdentifiableDouble(termId, weight));
                    }
                    Collections.sort(items, new DoubleValueDescSort<>());
                }
                List<List<IdentifiableDouble>> blocks = this.getBlocks(items);
                JsonArray arrTerms = new JsonArray();
                for (List<IdentifiableDouble> block : blocks) {
                    JsonArray arrBlock = new JsonArray();
                    for (IdentifiableDouble item : block) {
                        if (terms.contains(item.id())) {
                            Term term = terms.get(item.id());
                            JsonObject objTerm = new JsonObject();
                            objTerm.add("id", new JsonPrimitive(term.id()));
                            objTerm.add("name", new JsonPrimitive(term.name()));
                            objTerm.add("weight", new JsonPrimitive(item.toPlainString()));
                            arrBlock.add(objTerm);
                        }
                    }
                    arrTerms.add(arrBlock);
                }
                JsonObject doc = new JsonObject();
                doc.add("name", new JsonPrimitive(domainName));
                doc.add("columns", arrColumns);
                doc.add("terms", arrTerms);
                out.println(gson.toJson(doc));
            }
        }
    }
    
    private final static String COMMAND =
            "Usage:\n" +
            "  <eq-file>\n" +
            "  <term-file>\n" +
            "  <column-file>\n" +
            "  <strong-domain-file>\n" +
            "  <sample-size>\n" +
            "  <output-dir>";
    
    private final static Logger LOGGER = Logger
            .getLogger(ExportStrongDomains.class.getName());
    
    public static void main(String[] args) {
        
        System.out.println(Constants.NAME + " - Export Strong Domains - Version (" + Constants.VERSION + ")\n");
        
        if (args.length != 6) {
            System.out.println(COMMAND);
            System.exit(-1);
        }
        
        File eqFile = new File(args[0]);
        File termFile = new File(args[1]);
        File columnFile = new File(args[2]);
        File strongDomainFile = new File(args[3]);
        int sampleSize = Integer.parseInt(args[4]);
        File outputDir = new File(args[5]);

        try {
            new ExportStrongDomains()
                    .run(
                            eqFile,
                            termFile,
                            columnFile,
                            strongDomainFile,
                            sampleSize,
                            outputDir
                    );
        } catch (java.io.IOException ex) {
            LOGGER.log(Level.SEVERE, "RUN", ex);
            System.exit(-1);
        }
    }
}
