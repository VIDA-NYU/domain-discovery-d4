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

import org.opendata.curation.d4.telemetry.TelemetryPrinter;
import org.opendata.curation.d4.telemetry.TelemetryCollector;
import java.io.File;
import java.io.PrintWriter;
import java.math.BigDecimal;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.opendata.curation.d4.column.ExpandedColumnIndex;
import org.opendata.curation.d4.column.ExpandedColumnReader;
import org.opendata.curation.d4.column.ExpandedColumnStats;
import org.opendata.curation.d4.column.ExpandedColumnWriterFactory;
import org.opendata.curation.d4.column.ParallelColumnExpander;
import org.opendata.curation.d4.domain.DomainReader;
import org.opendata.curation.d4.domain.DomainSetStatsPrinter;
import org.opendata.curation.d4.domain.DomainWriter;
import org.opendata.curation.d4.domain.ParallelLocalDomainGenerator;
import org.opendata.curation.d4.domain.StrongDomainGenerator;
import org.opendata.curation.d4.signature.SignatureBlocksGenerator;
import org.opendata.curation.d4.signature.SignatureBlocksStats;
import org.opendata.core.constraint.Threshold;
import org.opendata.core.io.FileListReader;
import org.opendata.core.io.FileSystem;
import org.opendata.core.set.IdentifiableObjectSet;
import org.opendata.curation.d4.domain.Domain;
import org.opendata.curation.d4.domain.StrongDomain;
import org.opendata.curation.d4.domain.StrongDomainReader;
import org.opendata.curation.d4.domain.StrongDomainWriter;
import org.opendata.curation.d4.export.ExportStrongDomains;
import org.opendata.curation.d4.export.PrimaryDomainWriter;
import org.opendata.curation.d4.signature.SignatureBlocksReader;
import org.opendata.curation.d4.signature.SignatureBlocksStream;
import org.opendata.curation.d4.signature.SignatureBlocksWriterFactory;
import org.opendata.curation.d4.signature.trim.SignatureTrimmer;
import org.opendata.db.column.Column;
import org.opendata.db.eq.CompressedTermIndexGenerator;
import org.opendata.db.eq.EQIndex;
import org.opendata.db.term.TermIndexGenerator;
import org.opendata.db.term.TermIndexReader;
import org.opendata.db.tools.Dataset2ColumnsConverter;

/**
 * Complete D4 pipeline.
 * 
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class D4 {
    
    public void expandColumns(
            EQIndex nodeIndex,
            SignatureBlocksStream signatures,
            String trimmer,
            Threshold expandThreshold,
            int numberOfIterations,
            BigDecimal decreaseFactor,
            int threads,
            boolean verbose,
            TelemetryCollector telemetry,
            File outputFile
    ) throws java.io.IOException {
        
        IdentifiableObjectSet<Column> db = nodeIndex.columns();
        new ParallelColumnExpander(telemetry).run(
                nodeIndex,
                signatures,
                trimmer,
                db,
                db.keys(),
                expandThreshold,
                decreaseFactor,
                numberOfIterations,
                threads,
                verbose,
                new ExpandedColumnWriterFactory(outputFile, false)
        );

        if (verbose) {
            ExpandedColumnStats colStats = new ExpandedColumnStats();
            new ExpandedColumnReader(outputFile).stream(colStats);
            colStats.print();
        }
    }
    
    public void exportStrongDomains(
            File eqFile,
            File termFile,
            File columnFile,
            File strongDomainFile,
            int sampleSize,
            File outputDir
    ) throws java.io.IOException {

        new ExportStrongDomains().run(
                eqFile,
                termFile,
                columnFile,
                strongDomainFile,
                sampleSize,
                outputDir
        );
    }
    
    public void localDomains(
            EQIndex nodeIndex,
            File columnsFile,
            SignatureBlocksStream signatures,
            String trimmer,
            int threads,
            boolean verbose,
            TelemetryCollector telemetry,
            File outputFile
    ) throws java.io.IOException {
        
        ExpandedColumnIndex columnIndex = new ExpandedColumnIndex();
        new ExpandedColumnReader(columnsFile).stream(columnIndex);
        new ParallelLocalDomainGenerator(telemetry).run(
                nodeIndex,
                columnIndex,
                signatures,
                trimmer,
                threads,
                verbose,
                new DomainWriter(outputFile)
        );

        if (verbose) {
            DomainSetStatsPrinter localStats = new DomainSetStatsPrinter();
            new DomainReader(outputFile).stream(localStats);
            localStats.print();
        }
    }
    
    public void signatures(
            EQIndex nodeIndex,
            boolean fullSignatureConstraint,
            boolean ignoreLastDrop,
            int threads,
            boolean verbose,
            TelemetryCollector telemetry,
            File outputFile
    ) throws java.lang.InterruptedException, java.io.IOException {
        
        SignatureBlocksWriterFactory sigWriter;
        sigWriter = new SignatureBlocksWriterFactory(outputFile, false);
        new SignatureBlocksGenerator(telemetry).runWithMaxDrop(
                nodeIndex,
                new ConcurrentLinkedQueue<>(nodeIndex.keys().toList()),
                fullSignatureConstraint,
                ignoreLastDrop,
                threads,
                verbose,
                sigWriter
        );

        if (verbose) {
            SignatureBlocksStats sigStats = new SignatureBlocksStats();
            new SignatureBlocksReader(outputFile).stream(sigStats);
            sigStats.print();
        }
    }
    
    public void strongDomains(
            EQIndex nodeIndex,
            File localDomainFile,
            Threshold domainOverlapConstraint,
            BigDecimal supportFraction,
            int threads,
            boolean verbose,
            TelemetryCollector telemetry,
            File outputFile
    ) throws java.lang.InterruptedException, java.io.IOException {
        
        IdentifiableObjectSet<Domain> localDomains;
        localDomains = new DomainReader(localDomainFile).read();
        new StrongDomainGenerator(telemetry).run(
                nodeIndex,
                localDomains,
                domainOverlapConstraint,
                Threshold.getConstraint("GT0.1"),
                supportFraction,
                verbose,
                threads,
                new StrongDomainWriter(outputFile, localDomains)
        );

        if (verbose) {
            DomainSetStatsPrinter statsPrinter = new DomainSetStatsPrinter();
            statsPrinter.open();
            for (StrongDomain domain : new StrongDomainReader(outputFile).read()) {
                statsPrinter.consume(
                        new Domain(
                                domain.id(),
                                domain.members().keys(),
                                domain.columns()
                        )
                );
            }
            statsPrinter.close();
            statsPrinter.print();
        }
    }
    
    /**
     * Identifier for different steps in the D4 pipeline.
     */
    private static final String STEP_COMPRESS_TERMINDEX = "eqs";
    private static final String STEP_EXPAND_COLUMNS = "expand-columns";
    private static final String STEP_EXPORT_DOMAINS = "export";
    private static final String STEP_GENERATE_COLUMNS = "columns";
    private static final String STEP_LOCAL_DOMAINS = "local-domains";
    private static final String STEP_SIGNATURES = "signatures";
    private static final String STEP_STRONG_DOMAINS = "strong-domains";
    private static final String STEP_TERMINDEX = "term-index";

    private static final String COMMAND =
            "Usage:\n" +
            "  <command> [\n\n" +
            "      Data preparation\n" +
            "      ----------------\n" +
            "      " + STEP_GENERATE_COLUMNS + "\n" +
            "      " + STEP_TERMINDEX + "\n" +
            "      " + STEP_COMPRESS_TERMINDEX + "\n\n" +
            "      D4 pipeline\n" +
            "      -----------\n" +
            "      " + STEP_SIGNATURES + "\n" +
            "      " + STEP_EXPAND_COLUMNS + "\n" +
            "      " + STEP_LOCAL_DOMAINS + "\n" +
            "      " + STEP_STRONG_DOMAINS + "\n\n" +
            "      Explore Results\n" +
            "      ---------------\n" +
            "      " + STEP_EXPORT_DOMAINS + "\n\n" +
            "  ] <args>";

    private static final String UNKNOWN = "Use --help to see a list steps in the D4 pipeline";
    
    private static final Logger LOGGER = Logger.getLogger(D4.class.getName());
    
    public static void main(String[] args) {
        
        System.out.println(Constants.NAME + " - Version (" + Constants.VERSION + ")\n");

        if (args.length == 0) {
            System.out.println(UNKNOWN);
            System.exit(-1);
        } else if (args.length == 1) {
            if (args[0].equals("--help")) {
                System.out.println(COMMAND);
                System.exit(0);                
            }
        }
        
        String command = args[0];
        if (command.equals(STEP_GENERATE_COLUMNS)) {
            // ----------------------------------------------------------------
            // GENERATE COLUMN FILES
            // ----------------------------------------------------------------
            CLP params = new CLP(
                    new Parameter[] {
                        new Parameter("input", "<directory> [default: 'tsv']"),
                        new Parameter("metadata", "<file> [default: 'columns.tsv']"),
                        new Parameter("toupper", "<boolean> [default: true]"),
                        new Parameter("verbose", "<boolean> [default: true]"),
                        new Parameter("output", "<directory> [default: 'columns']")
                    },
                    args
            );
            File inputDir = params.getAsFile("input", "tsv");
            File outputFile = params.getAsFile("metadata", "columns.tsv");
            boolean toUpper = params.getAsBool("toupper", true);
            boolean verbose = params.getAsBool("verbose", true);
            File outputDir = params.getAsFile("output", "columns");
            try (PrintWriter out = FileSystem.openPrintWriter(outputFile)) {
                List<File> files = new FileListReader(new String[]{".csv", ".tsv"})
                        .listFiles(inputDir);
                new Dataset2ColumnsConverter(outputDir, out, toUpper, verbose)
                        .run(files);
            } catch (java.lang.InterruptedException | java.io.IOException ex) {
                LOGGER.log(Level.SEVERE, "COLUMN FILES", ex);
                System.exit(-1);
            }
        } else if (command.equals(STEP_TERMINDEX)) {
            // ----------------------------------------------------------------
            // GENERATE TERM INDEX
            // ----------------------------------------------------------------
            CLP params = new CLP(
                    new Parameter[] {
                        new Parameter(
                                "input",
                                "<directory | file> [default: 'columns']"
                        ),
                        new Parameter("textThreshold", "<constraint> [default: 'GT0.5']"),
                        new Parameter("membuffer", "<int> [default: 10000000]"),
                        new Parameter("verbose", "<boolean> [default: true]"),
                        new Parameter("output", "<file> [default: 'text-columns.txt']")
                    },
                    args
            );
            File inputDir = params.getAsFile("input", "columns");
            Threshold threshold = params.getAsConstraint("textThreshold", "GT0.5");
            int bufferSize = params.getAsInt("membuffer", 10000000);
            boolean verbose = params.getAsBool("verbose", true);
            File outputFile = params.getAsFile("output", "term-index.txt.gz");
            try {
                new TermIndexGenerator().run(
                        new FileListReader(".txt").listFiles(inputDir),
                        threshold,
                        bufferSize,
                        verbose,
                        outputFile
                );
            } catch (java.io.IOException ex) {
                LOGGER.log(Level.SEVERE, "TERM INDEX", ex);
                System.exit(-1);
            }
        } else if (command.equals(STEP_COMPRESS_TERMINDEX)) {
            // ----------------------------------------------------------------
            // GENERATE EQUIVALENCE CLASSES
            // ----------------------------------------------------------------
            CLP params = new CLP(
                    new Parameter[] {
                        new Parameter("input", "<file> [default: 'term-index.txt.gz']"),
                        new Parameter("verbose", "<boolean> [default: true]"),
                        new Parameter(
                                "output",
                                "<file> [default: 'compressed-term-index.txt.gz']"
                        )
                    },
                    args
            );
            File inputFile = params.getAsFile("input", "term-index.txt.gz");
            boolean verbose = params.getAsBool("verbose", true);
            File outputFile = params.getAsFile("output", "compressed-term-index.txt.gz");     
            try (PrintWriter out = FileSystem.openPrintWriter(outputFile)) {
                new TermIndexReader(inputFile)
                        .read(new CompressedTermIndexGenerator(out, verbose));
            } catch (java.io.IOException ex) {
                LOGGER.log(Level.SEVERE, "EQUIVALENCE CLASSES", ex);
                System.exit(-1);
            }
        } else if (command.equals(STEP_SIGNATURES)) {
            // ----------------------------------------------------------------
            // COMPUTE SIGNATURES
            // ----------------------------------------------------------------
            CLP params = new CLP(
                    new Parameter[] {
                        new Parameter(
                                "eqs",
                                "<file> [default: 'compressed-term-index.txt.gz']"
                        ),
                        new Parameter("threads", "<int> [default: 6]"),
                        new Parameter("verbose", "<boolean> [default: true]"),
                        new Parameter("signatures", "<file> [default: 'signatures.txt.gz']")
                    },
                    args
            );
            File eqFile = params.getAsFile("eqs", "compressed-term-index.txt.gz");
            int threads = params.getAsInt("threads", 6);
            boolean verbose = params.getAsBool("verbose", true);
            File signatureFile = params.getAsFile("signatures", "signatures.txt.gz");     
            boolean fullSignatureConstraint = false;
            boolean ignoreLastDrop = true;
            try {
                new D4().signatures(
                        new EQIndex(eqFile),
                        fullSignatureConstraint,
                        ignoreLastDrop,
                        threads,
                        verbose,
                        new TelemetryPrinter(),
                        signatureFile
                );
            } catch (java.lang.InterruptedException | java.io.IOException ex) {
                LOGGER.log(Level.SEVERE, "SIGNATURES", ex);
                System.exit(-1);
            }
        } else if (command.equals(STEP_EXPAND_COLUMNS)) {
            // ----------------------------------------------------------------
            // EXPAND COLUMNS
            // ----------------------------------------------------------------
            CLP params = new CLP(
                    new Parameter[] {
                        new Parameter(
                                "eqs",
                                "<file> [default: 'compressed-term-index.txt.gz']"
                        ),
                        new Parameter("signatures", "<file> [default: 'signatures.txt.gz']"),
                        new Parameter(
                                "trimmer",
                                "<string> [default: " + SignatureTrimmer.CENTRIST + "]"
                        ),
                        new Parameter("expandThreshold", "<constraint> [default: 'GT0.25']"),
                        new Parameter("decrease", "<double> [default: 0.05]"),
                        new Parameter("iterations", "<int> [default: 5]"),
                        new Parameter("threads", "<int> [default: 6]"),
                        new Parameter("verbose", "<boolean> [default: true]"),
                        new Parameter("columns", "<file> [default: 'expanded-columns.txt.gz']")
                    },
                    args
            );
            File eqFile = params.getAsFile("eqs", "compressed-term-index.txt.gz");
            File signatureFile = params.getAsFile("signatures", "signatures.txt.gz");     
            String trimmer = params.getAsString("trimmer", SignatureTrimmer.CENTRIST);
            Threshold expandThreshold = params.getAsConstraint("expandThreshold", "GT0.25");
            BigDecimal decreaseFactor = params
                    .getAsBigDecimal("decrease", new BigDecimal("0.05"));
            int numberOfIterations = params.getAsInt("iterations", 5);
            int threads = params.getAsInt("threads", 6);
            boolean verbose = params.getAsBool("verbose", true);
            File columnsFile = params.getAsFile("columns", "expanded-columns.txt.gz");     
            try {
                new D4().expandColumns(
                        new EQIndex(eqFile),
                        new SignatureBlocksReader(signatureFile),
                        trimmer,
                        expandThreshold,
                        numberOfIterations,
                        decreaseFactor,
                        threads,
                        verbose,
                        new TelemetryPrinter(),
                        columnsFile
                );
            } catch (java.io.IOException ex) {
                LOGGER.log(Level.SEVERE, "EXPAND COLUMNS", ex);
                System.exit(-1);
            }
        } else if (command.equals(STEP_LOCAL_DOMAINS)) {
            // ----------------------------------------------------------------
            // DISCOVER LOCAL DOMAINS
            // ----------------------------------------------------------------
            CLP params = new CLP(
                    new Parameter[] {
                        new Parameter(
                                "eqs",
                                "<file> [default: 'compressed-term-index.txt.gz']"
                        ),
                        new Parameter("columns", "<file> [default: 'expanded-columns.txt.gz']"),
                        new Parameter("signatures", "<file> [default: 'signatures.txt.gz']"),
                        new Parameter(
                                "trimmer",
                                "<string> [default: " + SignatureTrimmer.CENTRIST + "]"
                        ),
                        new Parameter("threads", "<int> [default: 6]"),
                        new Parameter("verbose", "<boolean> [default: true]"),
                        new Parameter(
                                "localdomains",
                                "<file> [default: 'local-domains.txt.gz']"
                        )
                    },
                    args
            );
            File eqFile = params.getAsFile("eqs", "compressed-term-index.txt.gz");
            File columnsFile = params.getAsFile("columns", "expanded-columns.txt.gz");     
            File signatureFile = params.getAsFile("signatures", "signatures.txt.gz");     
            String trimmer = params.getAsString("trimmer", SignatureTrimmer.CENTRIST);
            int threads = params.getAsInt("threads", 6);
            boolean verbose = params.getAsBool("verbose", true);
            File localDomainFile = params.getAsFile("localdomains", "local-domains.txt.gz");
            try {
                new D4().localDomains(
                        new EQIndex(eqFile),
                        columnsFile,
                        new SignatureBlocksReader(signatureFile),
                        trimmer,
                        threads,
                        verbose,
                        new TelemetryPrinter(),
                        localDomainFile
                );
            } catch (java.io.IOException ex) {
                LOGGER.log(Level.SEVERE, "LOCAL DOMAINS", ex);
                System.exit(-1);
            }
        } else if (command.equals(STEP_STRONG_DOMAINS)) {
            // ----------------------------------------------------------------
            // PRUNE STRONG DOMAINS
            // ----------------------------------------------------------------
            CLP params = new CLP(
                    new Parameter[] {
                        new Parameter(
                                "eqs",
                                "<file> [default: 'compressed-term-index.txt.gz']"
                        ),
                        new Parameter(
                                "localdomains",
                                "<file> [default: 'local-domains.txt.gz']"
                        ),
                        new Parameter("domainOverlap",  "<constraint> [default: 'GT0.5']"),
                        new Parameter("supportFraction",  "<double> [default: 0.25]"),
                        new Parameter("threads", "<int> [default: 6]"),
                        new Parameter("verbose", "<boolean> [default: true]"),
                        new Parameter(
                                "strongdomains",
                                "<file> [default: 'strong-domains.txt.gz']"
                        ),
                    },
                    args
            );
            File eqFile = params.getAsFile("eqs", "compressed-term-index.txt.gz");
            File localDomainFile = params.getAsFile("localdomains", "local-domains.txt.gz");
            Threshold domainOverlapConstraint = params.getAsConstraint(UNKNOWN, "GT0.5");
            BigDecimal supportFraction = params
                    .getAsBigDecimal("supportFraction", new BigDecimal("0.25"));
            int threads = params.getAsInt("threads", 6);
            boolean verbose = params.getAsBool("verbose", true);
            File strongDomainFile = params.getAsFile("strongdomains", "strong-domains.txt.gz");
            try {
                new D4().strongDomains(
                        new EQIndex(eqFile),
                        localDomainFile,
                        domainOverlapConstraint,
                        supportFraction,
                        threads,
                        verbose,
                        new TelemetryPrinter(),
                        strongDomainFile
                );
            } catch (java.lang.InterruptedException | java.io.IOException ex) {
                LOGGER.log(Level.SEVERE, "STRONG DOMAINS", ex);
                System.exit(-1);
            }
        } else if (command.equals(STEP_EXPORT_DOMAINS)) {
            // ----------------------------------------------------------------
            // EXPORT
            // ----------------------------------------------------------------
            CLP params = new CLP(
                    new Parameter[] {
                        new Parameter(
                                "eqs",
                                "<file> [default: 'compressed-term-index.txt.gz']"
                        ),
                        new Parameter("terms", "<file> [default: 'term-index.txt.gz']"),
                        new Parameter("columns", "<file> [default: 'columns.tsv']"),
                        new Parameter(
                                "domains",
                                "<file> [default: 'strong-domains.txt.gz']"
                        ),
                        new Parameter("sampleSize", "<int> [default: 100]"),
                        new Parameter("writePrimary", "<boolean> [default: false]"),
                        new Parameter("output", "<direcory> [default: 'domains']"),
                    },
                    args
            );
            File eqFile = params.getAsFile("eqs", "compressed-term-index.txt.gz");
            File termFile = params.getAsFile("terms", "term-index.txt.gz");
            File columnsFile = params.getAsFile("columns", "columns.tsv");
            File domainsFile = params.getAsFile("domains", "strong-domains.txt.gz");
            int samleSize = params.getAsInt("sampleSize", 100);
            boolean writePrimary = params.getAsBool("writePrimary", false);
            File outputDir = params.getAsFile("output", "domains");
            try {
                new D4().exportStrongDomains(
                        eqFile,
                        termFile,
                        columnsFile,
                        domainsFile,
                        samleSize,
                        outputDir
                );
                if (writePrimary) {
                    new PrimaryDomainWriter().run(outputDir, outputDir);
                }
            } catch (java.io.IOException ex) {
                LOGGER.log(Level.SEVERE, "EXPORT DOMAINS", ex);
                System.exit(-1);
            }
        } else {
            System.out.println(UNKNOWN);
            System.exit(-1);
        }
    }
}
