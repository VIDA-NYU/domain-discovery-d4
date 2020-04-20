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
import java.io.File;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.opendata.core.io.FileSystem;
import org.opendata.core.object.Entity;
import org.opendata.core.set.EntitySet;
import org.opendata.core.set.HashIDSet;
import org.opendata.core.set.IdentifiableObjectSet;
import org.opendata.curation.d4.column.ExpandedColumn;
import org.opendata.curation.d4.column.ExpandedColumnIndex;
import org.opendata.curation.d4.column.ExpandedColumnReader;
import org.opendata.curation.d4.domain.Domain;
import org.opendata.curation.d4.domain.DomainReader;
import org.opendata.curation.d4.domain.StrongDomain;
import org.opendata.curation.d4.domain.StrongDomainReader;
import org.opendata.db.eq.EQIndex;

/**
 *
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class ExportColumns {
    
    public void run(
            EQIndex eqIndex,
            EntitySet terms,
            IdentifiableObjectSet<ExpandedColumn> columns,
            IdentifiableObjectSet<Domain> localDomains,
            IdentifiableObjectSet<StrongDomain> strongDomains,
            File outputDir
    ) throws java.io.IOException {
        
        FileSystem.createFolder(outputDir);
        
        for (ExpandedColumn column : columns) {
            List<Domain> columnLocalDomains = new ArrayList<>();
            List<Domain> columnStrongDomains = new ArrayList<>();
            for (Domain localDomain : localDomains) {
                if (localDomain.columns().contains(column.id())) {
                    boolean isStrong = false;
                    for (StrongDomain strongDomain : strongDomains) {
                        if (strongDomain.localDomains().contains(localDomain.id())) {
                            isStrong = true;
                            break;
                        }
                    }
                    columnLocalDomains.add(localDomain);
                    if (isStrong) {
                        columnStrongDomains.add(localDomain);
                    }
                }
            }
            File outputFile = FileSystem.joinPath(outputDir, column.id() + ".json");
            try (OutputStream out = FileSystem.openOutputFile(outputFile)) {
                JsonWriter writer;
                writer = new JsonWriter(new OutputStreamWriter(out, "UTF-8"));
                writer.beginObject();
                // Terms
                writer.name("terms");
                writer.beginObject();
                writer.name("original");
                writer.beginArray();
                for (int nodeId : column.originalNodes()) {
                    for (int termId : eqIndex.get(nodeId).terms()) {
                        this.writeTerm(terms.get(termId), writer);
                    }
                }
                writer.endArray();
                writer.name("expansion");
                writer.beginArray();
                for (int nodeId : column.expandedNodes()) {
                    for (int termId : eqIndex.get(nodeId).terms()) {
                        this.writeTerm(terms.get(termId), writer);
                    }
                }
                writer.endArray();
                writer.endObject();
                // Local Domains
                writer.name("localDomains");
                writer.beginArray();
                for (Domain domain : columnLocalDomains) {
                    writer.beginObject();
                    writer.name("id").value(domain.id());
                    writer.name("terms");
                    writer.beginArray();
                    for (int nodeId : domain) {
                        for (int termId : eqIndex.get(nodeId).terms()) {
                            writer.value(termId);
                        }
                    }
                    writer.endArray();
                    writer.endObject();
                }
                writer.endArray();
                // Strong Domains
                 writer.name("strongDomains");
                writer.beginArray();
                for (Domain domain : columnStrongDomains) {
                    writer.value(domain.id());
                }
                writer.endArray();
                writer.endObject();
                writer.flush();
            }
        }
    }
    
    private void writeTerm(Entity term, JsonWriter writer) throws java.io.IOException {
        
        writer.beginObject();
        writer.name("id").value(term.id());
        writer.name("value").value(term.name());
        writer.endObject();
    }
    
    private final static String COMMAND =
            "Usage:\n" +
            "  <eq-file>\n" +
            "  <term-index-file>\n" +
            "  <columns-file>\n" +
            "  <local-domains-file>\n" +
            "  <strong-domains-file>\n" +
            "  <output-dir>";
    
    private final static Logger LOGGER = Logger
            .getLogger(ExportColumns.class.getName());
    
    public static void main(String[] args) {
        
        if (args.length != 6) {
            System.out.println(COMMAND);
            System.exit(-1);
        }
        
        File eqFile = new File(args[0]);
        File termsFile = new File(args[1]);
        File columnsFile = new File(args[2]);
        File localDomainsFile = new File(args[3]);
        File strongDomainsFile = new File(args[4]);
        File outputDir = new File(args[5]);
        
        try {
            ExpandedColumnIndex columnIndex = new ExpandedColumnIndex();
            new ExpandedColumnReader(columnsFile)
                    .stream(columnIndex, new HashIDSet(columnsFile));
            new ExportColumns().run(
                    new EQIndex(eqFile),
                    new EntitySet(termsFile),
                    new ExpandedColumnReader(columnsFile).read(),
                    new DomainReader(localDomainsFile).read(),
                    new StrongDomainReader(strongDomainsFile).read(),
                    outputDir
            );
        } catch (java.io.IOException ex) {
            LOGGER.log(Level.SEVERE, "RUN", ex);
            System.exit(-1);
        }
    }
}
