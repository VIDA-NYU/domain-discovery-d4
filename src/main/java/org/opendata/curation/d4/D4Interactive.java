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
package org.opendata.curation.d4;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonObject;
import java.io.File;
import java.util.Scanner;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.opendata.core.set.IdentifiableObjectSet;
import org.opendata.curation.d4.column.ExpandedColumn;
import org.opendata.curation.d4.column.ExpandedColumnReader;
import org.opendata.curation.d4.domain.LocalDomainGenerator;
import org.opendata.curation.d4.signature.SignatureBlocksIndex;
import org.opendata.curation.d4.signature.SignatureBlocksReader;
import org.opendata.db.Database;
import org.opendata.db.eq.CompressedTermIndex;
import org.opendata.db.eq.CompressedTermIndexFile;
import org.opendata.db.term.TermIndexReader;

/**
 * Interactive D4.
 * 
 * @author @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class D4Interactive {
   
    private final static String COMMAND =
            "Usage:\n" +
            "  <eq-file>\n" +
            "  <term-file>\n" +
            "  <signature-file>\n" +
            "  <expanded-column-file>";
    
    private final static Logger LOGGER = Logger
            .getLogger(D4Interactive.class.getName());
    
    public static void main(String[] args) throws java.io.IOException {
        
        System.out.println(Constants.NAME + " (Interactive) - Version (" + Constants.VERSION + ")\n");

        if (args.length != 4) {
            System.out.println(COMMAND);
            System.exit(-1);
        }
        
        File eqFile = new File(args[0]);
        File termFile = new File(args[1]);
        File signatureFile = new File(args[2]);
        File columnsFile = new File(args[3]);
        
        CompressedTermIndex eqIndex = new CompressedTermIndexFile(eqFile);
        TermIndexReader termIndex = new TermIndexReader(termFile);
        
        DataManager d4 = new DataManager(eqIndex);
        Database db = new Database(eqIndex, termIndex);
        
        SignatureBlocksIndex signatures;
        signatures = new SignatureBlocksReader(signatureFile).read();
        
        IdentifiableObjectSet<ExpandedColumn> columns;
        columns = new ExpandedColumnReader(columnsFile).read();
        
        Scanner scanner = new Scanner(System.in);
        while (true) {
            System.out.print("D4 $> ");
            String[] tokens = scanner.nextLine().split("\\s+");
            if (tokens.length == 0) {
                continue;
            }
            try {
                switch (tokens[0]) {
                    case "exit":
                        System.out.println("See ya, mate!");
                        return;
                    case "local-domains":
                        int columnId = Integer.parseInt(tokens[1]);
                        String trimmer = D4Config.TRIMMER_CENTRIST;
                        if (tokens.length >= 3) {
                            trimmer = tokens[2].toUpperCase();
                        }
                        boolean originalOnly = false;
                        if (tokens.length == 4) {
                            originalOnly = Boolean.parseBoolean(tokens[3]);
                        }
                        LocalDomainGenerator domGen;
                        domGen = new LocalDomainGenerator(
                                db,
                                signatures,
                                d4.getEQTermCounts()
                        );
                        JsonObject doc = domGen.getLocalDomain(
                                columns.get(columnId),
                                d4.getSignatureTrimmerFactory(trimmer, originalOnly)
                        );
                        Gson gson = new GsonBuilder().setPrettyPrinting().create();
                        System.out.println(gson.toJson(doc));
                        break;
                    default:
                        LOGGER.log(Level.INFO, String.format("Unknown command: %s", tokens[0]));
                        break;
                }
            } catch (RuntimeException ex) {
                LOGGER.log(Level.WARNING, tokens[0], ex);
            }
        }
    }
}
