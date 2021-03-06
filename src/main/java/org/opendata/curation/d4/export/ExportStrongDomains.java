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
import java.util.HashMap;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.opendata.core.io.FileSystem;
import org.opendata.core.object.IdentifiableDouble;
import org.opendata.core.set.HashIDSet;
import org.opendata.core.set.IdentifiableObjectSet;
import org.opendata.curation.d4.Constants;
import org.opendata.curation.d4.domain.StrongDomain;
import org.opendata.curation.d4.domain.StrongDomainMember;
import org.opendata.curation.d4.domain.StrongDomainReader;
import org.opendata.db.Database;
import org.opendata.db.EQTerms;
import org.opendata.db.eq.CompressedTermIndex;
import org.opendata.db.eq.CompressedTermIndexFile;
import org.opendata.db.term.TermIndexReader;

/**
 * Write terms in a strong domain to file. Outputs a Json file for each strong
 * domain. the file contains the list of columns that the domains occurs in an
 * the equivalence classes (and their terms or a sample of them). Equivalence
 * classes are divided into blocks based on their weight which is derived from
 * the number of local domains in this strong domains that the equivalence
 * class belongs to.
 * 
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class ExportStrongDomains {
    
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
        CompressedTermIndex eqIndex = new CompressedTermIndexFile(eqFile);
        
        HashIDSet termFilter = new HashIDSet();
        for (StrongDomain domain : strongDomains) {
            for (StrongDomainMember node : domain.members()) {
                termFilter.add(node.id());
            }
        }
        
        // Read information about all terms
        HashMap<Integer, EQTerms> terms;
        terms = new Database(eqIndex, new TermIndexReader(termFile)).read(termFilter);
        
        // Write each domain to a separate file in the output directory
        Gson gson = new GsonBuilder().setPrettyPrinting().create();
        for (StrongDomain domain : strongDomains) {
            String filename = domain.id() + ".json";
            File outputFile = FileSystem.joinPath(outputDir, filename);
            try (PrintWriter out = FileSystem.openPrintWriter(outputFile)) {
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
                }
                List<List<IdentifiableDouble>> blocks = domain.getBlocksWithWeights();
                JsonArray arrTerms = new JsonArray();
                for (List<IdentifiableDouble> block : blocks) {
                    JsonArray arrBlock = new JsonArray();
                    for (IdentifiableDouble item : block) {
                        EQTerms eq = terms.get(item.id());
                        JsonObject obj = new JsonObject();
                        obj.add("id", new JsonPrimitive(eq.id()));
                        obj.add("termCount", new JsonPrimitive(eq.termCount()));
                        obj.add("weight", new JsonPrimitive(item.toPlainString()));
                        JsonArray eqTerms = new JsonArray();
                        for (String term : eq) {
                            eqTerms.add(new JsonPrimitive(term));
                        }
                        obj.add("terms", eqTerms);
                        arrBlock.add(obj);
                    }
                    arrTerms.add(arrBlock);
                }
                JsonObject doc = new JsonObject();
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
