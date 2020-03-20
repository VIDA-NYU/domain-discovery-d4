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

import java.io.File;
import java.math.BigDecimal;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.opendata.core.constraint.Threshold;
import org.opendata.core.io.FileSystem;
import org.opendata.curation.d4.domain.ExportStrongDomains;
import org.opendata.curation.d4.signature.SignatureBlocksIndex;
import org.opendata.curation.d4.signature.trim.TrimmerType;
import org.opendata.curation.d4.telemetry.TelemetryPrinter;
import org.opendata.db.eq.EQIndex;

/**
 * Run the domain discovery pipeline for D4 with a single command.
 * 
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class DiscoverDomains {
    
    /**
     * Run the D4 pipeline.
     * 
     * @param eqFile
     * @param termFile
     * @param trimmer
     * @param expandThreshold
     * @param numIterations
     * @param decreaseFactor
     * @param domainOverlapConstraint
     * @param supportFraction
     * @param runDir
     * @param threads
     * @param cleanUp
     * @throws java.lang.InterruptedException
     * @throws java.io.IOException 
     */
    public void run(
            File eqFile,
            File termFile,
            TrimmerType trimmer,
            Threshold expandThreshold,
            int numIterations,
            BigDecimal decreaseFactor,
            Threshold domainOverlapConstraint,
            BigDecimal supportFraction,
            File runDir,
            int threads,
            boolean cleanUp
    ) throws java.lang.InterruptedException, java.io.IOException {
        
        System.out.println("\n\n" + runDir.getName() + "\n\n");
        
        File signaturesFile = FileSystem.joinPath(runDir, "signatures.txt.gz");
        File columnsFile = FileSystem.joinPath(runDir, "expanded-columns.txt.gz");
        File localDomainsFile = FileSystem.joinPath(runDir, "local-domains.txt.gz");
        File strongDomainsFile = FileSystem.joinPath(runDir, "strong-domains.txt.gz");
        File domainsDir = FileSystem.joinPath(runDir, "domains");
        
        EQIndex eqIndex = new EQIndex(eqFile);
        
        D4 d4 = new D4();
        
        // GENERATE SIGNATURES
        SignatureBlocksIndex signatures;
        signatures = d4.signatures(
                eqIndex,
                false,
                true,
                threads,
                new TelemetryPrinter(),
                null
        );
        //signatures = new SignatureBlocksReader(signaturesFile).read();
        
        // EXPAND COLUMNS
        d4.expandColumns(
                eqIndex,
                signatures,
                trimmer,
                expandThreshold,
                numIterations,
                decreaseFactor,
                threads,
                new TelemetryPrinter(),
                columnsFile
        );
        
        // LOCAL DOMAINS
        d4.localDomains(
                eqIndex,
                columnsFile,
                signatures,
                trimmer,
                threads,
                new TelemetryPrinter(),
                localDomainsFile
        );
        
        // STRONG DOMAINS
        d4.strongDomains(
                eqIndex,
                localDomainsFile,
                domainOverlapConstraint,
                supportFraction,
                threads,
                new TelemetryPrinter(),
                strongDomainsFile
        );
        
        // EXPORT STRONG DOMAINS
        new ExportStrongDomains()
                .run(
                        eqFile,
                        termFile,
                        localDomainsFile,
                        strongDomainsFile,
                        domainsDir
                );
        
        if (cleanUp) {
            //signaturesFile.delete();
            columnsFile.delete();
        }
    }
    
    private final static String COMMAND =
            "Usage:\n" +
            "  <eq-file>\n" +
            "  <term-file>\n" +
            "  <threads>\n" +
            "  <clean-up> [true | false]\n" +
            "  <run-dir>";
    
    private final static Logger LOGGER = Logger
            .getLogger(DiscoverDomains.class.getName());
    
    public static void main(String[] args) {
        
        System.out.println("D4 - Discover Domains - Version (" + Constants.VERSION + ")\n");

        if (args.length != 5) {
            System.out.println(COMMAND);
            System.exit(-1);
        }
        
        File eqFile = new File(args[0]);
        File termFile = new File(args[1]);
        int threads = Integer.parseInt(args[2]);
        boolean cleanUp = Boolean.parseBoolean(args[3]);
        File runDir = new File(args[4]);
        
        try {
            new DiscoverDomains().run(
                    eqFile,
                    termFile,
                    TrimmerType.CENTRIST,
                    Threshold.getConstraint("GT0.5"),
                    1,
                    new BigDecimal("0.1"),
                    Threshold.getConstraint("GT0.5"),
                    new BigDecimal("0.25"),
                    runDir,
                    threads,
                    cleanUp
            );
        } catch (java.lang.InterruptedException | java.io.IOException ex) {
            LOGGER.log(Level.SEVERE, "RUN", ex);
            System.exit(-1);
        }
    }
}
