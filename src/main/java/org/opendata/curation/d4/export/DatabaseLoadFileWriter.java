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

import java.io.BufferedReader;
import java.io.File;
import java.io.PrintWriter;
import java.util.HashMap;
import org.opendata.curation.d4.domain.Domain;
import org.opendata.curation.d4.domain.DomainReader;
import org.opendata.core.object.AnyObjectFilter;
import org.opendata.core.object.ObjectFilter;
import org.opendata.core.io.FileSystem;
import org.opendata.core.set.IDSet;
import org.opendata.core.set.IdentifiableObjectSet;
import org.opendata.core.util.Counter;
import org.opendata.core.util.SimpleCounter;
import org.opendata.db.eq.EQIndex;
import org.opendata.db.eq.EQReader;
import org.opendata.db.term.TermIndexReader;

/**
 * Create all load files for a relational database containing information about
 * terms and equivalence classes.
 * 
 * Creates load files for the following tables:
 * 
 * term(id, value, type)
 * node(id)
 * term_node_map(term_id, node_id)
 * column_node_map(column_id, node_id)
 * node_containment(node_id1, node_id2) -> node 1 contains node 2
 * top_level_node(node_id)
 * 
 * The term threshold parameter allows to limit the number of terms per
 * equivalence class.
 * 
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class DatabaseLoadFileWriter {
    
    private void createIndex(String tableName, String columns, PrintWriter out) {
    
        out.println("CREATE INDEX ON " + tableName + "(" + columns + ");");
    }
    
    private void dropTable(String tableName, PrintWriter out) {
    
        out.println("DROP TABLE IF EXISTS " + tableName + ";\n");
    }
    
    private void loadFile(String tableName, String columns, File file, PrintWriter out) {
        
        out.println("\\copy " + tableName + "(" + columns + ") from './" + file.getName() + "' with delimiter E'\\t'\n");
    }
    
    private void primaryKey(String tableName, String columns, PrintWriter out) {
        
        out.println("ALTER TABLE " + tableName + " ADD PRIMARY KEY(" + columns + ");\n");
    }

    public HashMap<String, Counter> writeColumnsFile(
            File inputFile,
            ObjectFilter<Integer> filter,
            String tableName,
            File outputFile,
            PrintWriter script
    ) throws java.io.IOException {
        
        HashMap<String, Counter> datasetFilter = new HashMap<>();
        
        if (inputFile.exists()) {
            FileSystem.createParentFolder(outputFile);
            try (
                    BufferedReader in = FileSystem.openReader(inputFile);
                    PrintWriter out = FileSystem.openPrintWriter(outputFile)
            ) {
                String line;
                while ((line = in.readLine()) != null) {
                    String[] tokens = line.split("\t");
                    int colId = Integer.parseInt(tokens[0]);
                    if (filter.contains(colId)) {
                        String datasetId = tokens[2];
                        out.println(line + "\t0");
                        if (!datasetFilter.containsKey(datasetId)) {
                            datasetFilter.put(datasetId, new SimpleCounter(1));
                        } else {
                            datasetFilter.get(datasetId).inc();
                        }
                    }
                }
            }
            this.dropTable(tableName, script);
            script.println("CREATE TABLE " + tableName + " (");
            script.println("  id INTEGER NOT NULL,");
            script.println("  name VARCHAR(255) NOT NULL,");
            script.println("  dataset CHAR(9) NOT NULL,");
            script.println("  term_count INTEGER NULL");
            script.println(");\n");
            this.loadFile(tableName, "id, name, dataset, term_count", outputFile, script);
            this.primaryKey(tableName, "id", script);
        }
        
        return datasetFilter;
    }

    public void writeDatasetFile(
            File inputFile,
            File inputDir,
            HashMap<String, Counter> datasetStats,
            String tableName,
            File outputFile,
            PrintWriter script) throws java.io.IOException {
        
        if ((inputFile.exists()) && (inputDir.exists())) {
            try (
                    BufferedReader in = FileSystem.openReader(inputFile);
                    PrintWriter out = FileSystem.openPrintWriter(outputFile)
            ) {
                String line;
                while ((line = in.readLine()) != null) {
                    String[] tokens = line.split("\t");
                    String datasetId = tokens[1];
                    String datasetName = tokens[2];
                    if (datasetStats.containsKey(datasetId)) {
                        String filename = datasetId + ".tsv.gz";
                        File datasetFile = FileSystem.joinPath(inputDir, filename);
                        int rowCount = 0;
                        try (BufferedReader inDs = FileSystem.openReader(datasetFile)) {
                            inDs.readLine();
                            while (inDs.readLine() != null) {
                                rowCount++;
                            }
                        }
                        String outline = datasetId + "\t" +
                                datasetName + "\t" +
                                datasetStats.get(datasetId).value() + "\t" +
                                rowCount;
                        out.println(outline);
                    }
                }
            }
            this.dropTable(tableName, script);
            script.println("CREATE TABLE " + tableName + " (");
            script.println("  id CHAR(9) NOT NULL,");
            script.println("  name VARCHAR(512) NOT NULL,");
            script.println("  columns INTEGER NOT NULL,");
            script.println("  rows INTEGER NOT NULL");
            script.println(");\n");
            this.loadFile(tableName, "id, name, columns, rows", outputFile, script);
            this.primaryKey(tableName, "id", script);
        }
    }
    
    public void writeDomainFile(
            File inputFile,
            String tableName,
            File outputFile,
            PrintWriter script
    ) throws java.io.IOException {

        this.writeIdentifiableIDSetFile(
                inputFile,
                tableName,
                "domain_id",
                "node_id",
                outputFile,
                script
        );
    }

    public void writeDatabaseDomainFile(
            File strongDomainFile,
            File localDomainFile,
            File eqFile,
            String domainTableName,
            String domainColumnTableName,
            String domainTermTableName,
            String annotationTableName,
            ObjectFilter<Integer> termFilter,
            File outputDomainFile,
            File outputDomainColumnFile,
            File outputDomainTermFile,
            PrintWriter script
    ) throws java.io.IOException {

        // Domains
        FileSystem.createParentFolder(outputDomainFile);
        FileSystem.createParentFolder(outputDomainColumnFile);
        FileSystem.createParentFolder(outputDomainTermFile);
        
        EQIndex nodes = new EQIndex(eqFile);

        IdentifiableObjectSet<Domain> strongDomains;
        strongDomains = new DomainReader(strongDomainFile).read();
        
        IdentifiableObjectSet<Domain> localDomains;
        localDomains = new DomainReader(localDomainFile).read();
        
        try (
                PrintWriter outDomain = FileSystem.openPrintWriter(outputDomainFile);
                PrintWriter outColumn = FileSystem.openPrintWriter(outputDomainColumnFile);
                PrintWriter outTerm = FileSystem.openPrintWriter(outputDomainTermFile)
        ) {
            for (Domain domain : strongDomains) {
                int domainId = domain.id();
                int termCount = 0;
                for (int nodeId : domain) {
                    for (int termId : nodes.get(nodeId).terms()) {
                        if (termFilter.contains(termId)) {
                            outTerm.println(domainId + "\t" + termId);
                            termCount++;
                        }
                    }
                }
                for (int columnId : domain.columns()) {
                    outColumn.println(domainId + "\t" + columnId);
                }
                outDomain.println(
                        domainId + "\t" +
                        termCount + "\t" +
                        domain.columns().length() + "\t" +
                        "TRUE"
                );
            }
            Counter domIdFact = new SimpleCounter(strongDomains.getMaxId() + 1);
            for (Domain localDomain : localDomains) {
                IDSet columns = localDomain.columns();
                for (Domain strongDomain : strongDomains) {
                    if (strongDomain.columns().overlaps(columns)) {
                        if (localDomain.sameSetAs(strongDomain)) {
                            columns = columns.difference(strongDomain.columns());
                            if (columns.isEmpty()) {
                                break;
                            }
                        }
                    }
                }
                if (!columns.isEmpty()) {
                    int domainId = domIdFact.inc();
                    int termCount = 0;
                    for (int nodeId : localDomain) {
                        for (int termId : nodes.get(nodeId).terms()) {
                            if (termFilter.contains(termId)) {
                                outTerm.println(domainId + "\t" + termId);
                                termCount++;
                            }
                        }
                    }
                    for (int columnId : columns) {
                        outColumn.println(domainId + "\t" + columnId);
                    }
                    outDomain.println(
                            domainId + "\t" +
                            termCount + "\t" +
                            columns.length() + "\t" +
                            "FALSE"
                    );
                }
            }
        }
        
        
        this.dropTable(domainTableName, script);
        script.println("CREATE TABLE " + domainTableName + " (");
        script.println("  domain_id INTEGER NOT NULL,");
        script.println("  term_count INTEGER NOT NULL,");
        script.println("  column_count INTEGER NOT NULL,");
        script.println("  is_strong BOOLEAN NOT NULL");
        script.println(");\n");
        this.loadFile(domainTableName, "domain_id, term_count, column_count, is_strong", outputDomainFile, script);
        this.primaryKey(domainTableName, "domain_id", script);
        script.println();
        // Domains column mapping
        this.dropTable(domainColumnTableName, script);
        script.println("CREATE TABLE " + domainColumnTableName + " (");
        script.println("  domain_id INTEGER NOT NULL,");
        script.println("  column_id INTEGER NOT NULL");
        script.println(");\n");
        this.loadFile(domainColumnTableName, "domain_id, column_id", outputDomainColumnFile, script);
        this.primaryKey(domainColumnTableName, "domain_id, column_id", script);
        this.createIndex(domainColumnTableName, "domain_id", script);
        this.createIndex(domainColumnTableName, "column_id", script);
        // Domains term mapping
        this.dropTable(domainTermTableName, script);
        script.println("CREATE TABLE " + domainTermTableName + " (");
        script.println("  domain_id INTEGER NOT NULL,");
        script.println("  term_id INTEGER NOT NULL");
        script.println(");\n");
        this.loadFile(domainTermTableName, "domain_id, term_id", outputDomainTermFile, script);
        this.primaryKey(domainTermTableName, "domain_id, term_id", script);
        this.createIndex(domainTermTableName, "domain_id", script);
        script.println();
        // Annotation table
        this.dropTable(annotationTableName, script);
        script.println("CREATE TABLE " + annotationTableName + " (");
        script.println("  uri VARCHAR(128) NOT NULL,");
        script.println("  value TEXT NOT NULL");
        script.println(");\n");
    }

    private void writeIdentifiableIDSetFile(
            File inputFile,
            String tableName,
            String colName1,
            String colName2,
            File outputFile,
            PrintWriter script
    ) throws java.io.IOException {

        if (inputFile.exists()) {
            FileSystem.createParentFolder(outputFile);
	    new TSVFileWriter().convertIdentifiableIDSets(inputFile, outputFile);
	    this.dropTable(tableName, script);
            script.println("CREATE TABLE " + tableName + " (");
            script.println("  " + colName1 + " INTEGER NOT NULL,");
            script.println("  " + colName2 + " INTEGER NOT NULL");
            script.println(");\n");
            this.loadFile(tableName, colName1 + ", " + colName2, outputFile, script);
            this.primaryKey(tableName, colName1 + ", " + colName2, script);
            this.createIndex(tableName, colName1, script);
            this.createIndex(tableName, colName2, script);
            script.println();
        }
    }

    public void writeRobustSignaturesFile(
            File inputFile,
            String tableName,
            File outputFile,
            PrintWriter script
    ) throws java.io.IOException {

        this.writeIdentifiableIDSetFile(
                inputFile,
                tableName,
                "node_id",
                "member_id",
                outputFile,
                script
        );
    }

    public void writeTermsAndEquivalenceClasses(
            File eqFile,
            File termIndexFile,
            String termTableName,
            String columnTermMapTableName,
            String columnTableName,
            //String termNodeMapTableName,
            //String columnNodeMapTableName,
            int termThreshold,
            int valueLengthThreshold,
            File outputTermFile,
            File outputColumnTermMapFile,
            //File outputTermNodeMapFile,
            //File outputColumnNodeMapFile,
            PrintWriter script
    ) throws java.io.IOException {
        
        // Create parent folders for output files if they don't exist
        FileSystem.createParentFolder(outputTermFile);
        FileSystem.createParentFolder(outputColumnTermMapFile);
        //FileSystem.createParentFolder(outputTermNodeMapFile);
        //FileSystem.createParentFolder(outputColumnNodeMapFile);
        
        EQReader reader = new EQReader(eqFile);
        
	ObjectFilter<Integer> termFilter;
        
        //try (
                //PrintWriter outNodeTermMap = FileSystem.openPrintWriter(outputTermNodeMapFile);
                //PrintWriter outColumnNodeMap = FileSystem.openPrintWriter(outputColumnNodeMapFile)
        //) {
            // Write equivalence classe mapping files. Get term filter if term
            // threshold is not negative.
            if (termThreshold >= 0) {
                TermCollector consumer = new TermCollector(termThreshold);
                reader.stream(consumer);
                termFilter = consumer.terms();
            } else {
                //reader.stream(new EQWriter(outNodeTermMap, outColumnNodeMap));
                termFilter = new AnyObjectFilter();
            }
        //}

        //
        // Write term file
        //
        int maxTermLength = 0;
        try (
                PrintWriter outTerms = FileSystem.openPrintWriter(outputTermFile);
                PrintWriter outColumnTermMap = FileSystem.openPrintWriter(outputColumnTermMapFile)
        ) {
            TermFileWriter writer = new TermFileWriter(
                    termFilter,
                    valueLengthThreshold,
                    outTerms,
                    outColumnTermMap
            );
            new TermIndexReader(termIndexFile).read(writer);
            maxTermLength = writer.maxLength();
        }
        
        System.out.println("LONGEST TERM IS " + maxTermLength);
        
        this.dropTable(termTableName, script);
        script.println("CREATE TABLE " + termTableName + "(");
        script.println("  id INTEGER NOT NULL,");
        script.println("  value VARCHAR(" + maxTermLength + ") NOT NULL");
        script.println(");\n");
        this.loadFile(termTableName, "id, value", outputTermFile, script);
        this.primaryKey(termTableName, "id", script);
        script.println("ALTER TABLE " + termTableName + " ADD UNIQUE(value);\n");
        
        this.dropTable(columnTermMapTableName, script);
        script.println("CREATE TABLE " + columnTermMapTableName + " (");
        script.println("  column_id INTEGER NOT NULL,");
        script.println("  term_id INTEGER NOT NULL");
        script.println(");\n");
        this.loadFile(columnTermMapTableName, "column_id, term_id", outputColumnTermMapFile, script);
        this.primaryKey(columnTermMapTableName, "column_id, term_id", script);
        this.createIndex(columnTermMapTableName, "column_id", script);
        this.createIndex(columnTermMapTableName, "term_id", script);
        script.println();

        script.println();
        String sql = 
                "UPDATE " + columnTableName + " SET term_count = q.tc FROM (" +
                "SELECT column_id, COUNT(*) tc FROM " + columnTermMapTableName + " " +
                "GROUP BY column_id) AS q WHERE q.column_id = " + columnTableName + ".id;";
        script.println(sql);
        
        //this.dropTable(termNodeMapTableName, script);
        //script.println("CREATE TABLE " + termNodeMapTableName + " (");
        //script.println("  node_id INTEGER NOT NULL,");
        //script.println("  term_id INTEGER NOT NULL");
        //script.println(");\n");
        //this.loadFile(termNodeMapTableName, "node_id, term_id", outputTermNodeMapFile, script);
        //this.primaryKey(termNodeMapTableName, "term_id", script);
        //this.createIndex(termNodeMapTableName, "node_id", script);
        //script.println();
        
        //this.dropTable(columnNodeMapTableName, script);
        //script.println("CREATE TABLE " + columnNodeMapTableName + " (");
        //script.println("  column_id INTEGER NOT NULL,");
        //script.println("  node_id INTEGER NOT NULL");
        //script.println(");\n");
        //this.loadFile(columnNodeMapTableName, "column_id, node_id", outputColumnNodeMapFile, script);
        //this.primaryKey(columnNodeMapTableName, "column_id, node_id", script);
        //this.createIndex(columnNodeMapTableName, "column_id", script);
        //this.createIndex(columnNodeMapTableName, "node_id", script);
        //script.println();
    }
}
