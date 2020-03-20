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

import java.io.BufferedWriter;
import org.opendata.curation.d4.telemetry.TelemetryPrinter;
import org.opendata.curation.d4.telemetry.TelemetryCollector;
import java.io.File;
import java.io.PrintWriter;
import java.math.BigDecimal;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
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
import org.opendata.curation.d4.signature.trim.SignatureTrimmerFactory;
import org.opendata.curation.d4.signature.trim.TrimmerType;
import org.opendata.core.constraint.Threshold;
import org.opendata.core.io.FileListReader;
import org.opendata.core.io.FileSystem;
import org.opendata.core.set.IdentifiableObjectSet;
import org.opendata.curation.d4.domain.Domain;
import org.opendata.curation.d4.domain.StrongDomain;
import org.opendata.curation.d4.domain.StrongDomainReader;
import org.opendata.curation.d4.domain.StrongDomainWriter;
import org.opendata.curation.d4.export.DomainsExportWriter;
import org.opendata.curation.d4.signature.SignatureBlocksIndex;
import org.opendata.curation.d4.signature.SignatureBlocksIndexFactory;
import org.opendata.curation.d4.signature.SignatureBlocksReader;
import org.opendata.curation.d4.signature.SignatureBlocksWriter;
import org.opendata.db.column.Column;
import org.opendata.db.column.TextColumnFinder;
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
            SignatureBlocksIndex signatures,
            TrimmerType trimmer,
            Threshold expandThreshold,
            int numberOfIterations,
            BigDecimal decreaseFactor,
            int threads,
            TelemetryCollector telemetry,
            File outputFile
    ) throws java.io.IOException {
        
        System.out.println("\n-- COLUMN EXPANSION\n");
        
        IdentifiableObjectSet<Column> db = nodeIndex.columns();
        new ParallelColumnExpander(telemetry).run(
                nodeIndex,
                signatures,
                new SignatureTrimmerFactory(nodeIndex, trimmer),
                db,
                db.keys(),
                expandThreshold,
                decreaseFactor,
                numberOfIterations,
                threads,
                new ExpandedColumnWriterFactory(outputFile, false)
        );

        ExpandedColumnStats colStats = new ExpandedColumnStats();
        new ExpandedColumnReader(outputFile).stream(colStats);
        colStats.print();
    }
    
    public void localDomains(
            EQIndex nodeIndex,
            File columnsFile,
            SignatureBlocksIndex signatures,
            TrimmerType trimmer,
            int threads,
            TelemetryCollector telemetry,
            File outputFile
    ) throws java.io.IOException {
        
        System.out.println("\n-- LOCAL DOMAINS\n");

        ExpandedColumnIndex columnIndex = new ExpandedColumnIndex();
        new ExpandedColumnReader(columnsFile).stream(columnIndex);
        new ParallelLocalDomainGenerator(telemetry).run(
                nodeIndex,
                columnIndex,
                signatures,
                trimmer,
                threads,
                new DomainWriter(outputFile)
        );

        DomainSetStatsPrinter localStats = new DomainSetStatsPrinter();
        new DomainReader(outputFile).stream(localStats);
        localStats.print();
    
    }
    
    public SignatureBlocksIndex signatures(
            EQIndex nodeIndex,
            boolean fullSignatureConstraint,
            boolean ignoreLastDrop,
            int threads,
            TelemetryCollector telemetry,
            File outputFile
    ) throws java.lang.InterruptedException, java.io.IOException {
        
        System.out.println("\n-- SIGNATURE BLOCKS\n");
        
        SignatureBlocksIndexFactory sigBlocksIndex;
        sigBlocksIndex = new SignatureBlocksIndexFactory();
        new SignatureBlocksGenerator(telemetry).runWithMaxDrop(
                nodeIndex,
                new ConcurrentLinkedQueue<>(nodeIndex.keys().toList()),
                fullSignatureConstraint,
                ignoreLastDrop,
                threads,
                sigBlocksIndex
        );

        if (outputFile != null) {
            new SignatureBlocksWriter(outputFile).write(sigBlocksIndex.signatures());
        }
        SignatureBlocksStats sigStats = new SignatureBlocksStats();
        sigBlocksIndex.signatures().stream(sigStats);
        sigStats.print();
        
        return sigBlocksIndex.signatures();
    }
    
    public void strongDomains(
            EQIndex nodeIndex,
            File localDomainFile,
            Threshold domainOverlapConstraint,
            BigDecimal supportFraction,
            int threads,
            TelemetryCollector telemetry,
            File outputFile
    ) throws java.lang.InterruptedException, java.io.IOException {
        
        System.out.println("\n-- STRONG DOMAINS\n");

        IdentifiableObjectSet localDomains;
        localDomains =new DomainReader(localDomainFile).read();
        new StrongDomainGenerator(telemetry).run(
                nodeIndex,
                localDomains,
                domainOverlapConstraint,
                Threshold.getConstraint("GT0.1"),
                supportFraction,
                true,
                threads,
                new StrongDomainWriter(outputFile, localDomains)
        );

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
    
    /**
     * Identifier for different steps in the D4 pipeline.
     */
    private static final String STEP_COMPRESS_TERMINDEX = "eqs";
    private static final String STEP_EXPAND_COLUMNS = "expand-columns";
    private static final String STEP_EXPORT = "export";
    private static final String STEP_GENERATE_COLUMNS = "columns";
    private static final String STEP_LOCAL_DOMAINS = "local-domains";
    private static final String STEP_SIGNATURES = "signatures";
    private static final String STEP_STRONG_DOMAINS = "strong-domains";
    private static final String STEP_TERMINDEX = "term-index";
    private static final String STEP_TEXTCOLUMNS = "text-columns";

    private static final String COMMAND =
            "Usage:\n" +
            "  <command> [\n\n" +
            "      Data preparation\n" +
            "      ----------------\n" +
            "      " + STEP_GENERATE_COLUMNS + "\n" +
            "      " + STEP_TEXTCOLUMNS + "\n" +
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
            "      " + STEP_EXPORT + "\n\n" +
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
                        new Parameter("output", "<directory> [default: 'columns']")
                    },
                    args
            );
            File inputDir = params.getAsFile("input", "tsv");
            File outputFile = params.getAsFile("metadata", "columns.tsv");
            boolean toUpper = params.getAsBool("toupper", true);
            File outputDir = params.getAsFile("output", "columns");
            try (BufferedWriter out = FileSystem.openBufferedWriter(outputFile)) {
                CSVPrinter csvPrinter = new CSVPrinter(out, CSVFormat.TDF);
                List<File> files = new FileListReader(new String[]{".csv", ".tsv"})
                        .listFiles(inputDir);
                new Dataset2ColumnsConverter(outputDir, csvPrinter, toUpper)
                        .run(files);
            } catch (java.io.IOException ex) {
                LOGGER.log(Level.SEVERE, "COLUMN FILES", ex);
                System.exit(-1);
            }
        } else if (command.equals(STEP_TEXTCOLUMNS)) {
            // ----------------------------------------------------------------
            // IDENTIFY TEXT COLUMNS
            // ----------------------------------------------------------------
            CLP params = new CLP(
                    new Parameter[] {
                        new Parameter("input", "<directory> [default: 'columns']"),
                        new Parameter("threshold", "<constraint> [default: 'GT0.5']"),
                        new Parameter("output", "<file> [default: 'text-columns.txt']")
                    },
                    args
            );
            File inputDir = params.getAsFile("input", "columns");
            Threshold threshold = params.getAsConstraint("threshold", "GT0.5");
            File outputFile = params.getAsFile("output", "text-columns.txt");
            try (PrintWriter out = FileSystem.openPrintWriter(outputFile)) {
                new TextColumnFinder().run(
                        new FileListReader(".txt").listFiles(inputDir),
                        threshold,
                        out
                );
            } catch (java.io.IOException ex) {
                LOGGER.log(Level.SEVERE, "TEXT COLUMNS", ex);
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
                                "<directory | file> [default: 'text-columns.txt']"
                        ),
                        new Parameter("membuffer", "<int> [default: 10000000]"),
                        new Parameter("output", "<file> [default: 'text-columns.txt']")
                    },
                    args
            );
            File inputDir = params.getAsFile("input", "text-columns.txt");
            int bufferSize = params.getAsInt("membuffer", 10000000);
            File outputFile = params.getAsFile("output", "term-index.txt.gz");
            try {
                new TermIndexGenerator().run(
                        new FileListReader(".txt").listFiles(inputDir),
                        bufferSize,
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
                        new Parameter(
                                "output",
                                "<file> [default: 'compressed-term-index.txt.gz']"
                        )
                    },
                    args
            );
            File inputFile = params.getAsFile("input", "term-index.txt.gz");
            File outputFile = params.getAsFile("output", "compressed-term-index.txt.gz");     
            try (PrintWriter out = FileSystem.openPrintWriter(outputFile)) {
                new TermIndexReader(inputFile)
                        .read(new CompressedTermIndexGenerator(out));
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
                        new Parameter("signatures", "<file> [default: 'signatures.txt.gz']")
                    },
                    args
            );
            File eqFile = params.getAsFile("eqs", "compressed-term-index.txt.gz");
            int threads = params.getAsInt("threads", 6);
            File signatureFile = params.getAsFile("signatures", "signatures.txt.gz");     
            boolean fullSignatureConstraint = false;
            boolean ignoreLastDrop = true;
            try {
                new D4().signatures(
                        new EQIndex(eqFile),
                        fullSignatureConstraint,
                        ignoreLastDrop,
                        threads,
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
                                "<string> [default: " + TrimmerType.CENTRIST.toString() + "]"
                        ),
                        new Parameter("expandThreshold", "<constraint> [default: 'GT0.25']"),
                        new Parameter("decrease", "<double> [default: 0.05]"),
                        new Parameter("iterations", "<int> [default: 5]"),
                        new Parameter("threads", "<int> [default: 6]"),
                        new Parameter("columns", "<file> [default: 'expanded-columns.txt.gz']")
                    },
                    args
            );
            File eqFile = params.getAsFile("eqs", "compressed-term-index.txt.gz");
            File signatureFile = params.getAsFile("signatures", "signatures.txt.gz");     
            TrimmerType trimmer = TrimmerType
                .valueOf(params.getAsString("trimmer", TrimmerType.CENTRIST.toString()));
            Threshold expandThreshold = params.getAsConstraint("expandThreshold", "GT0.25");
            BigDecimal decreaseFactor = params
                    .getAsBigDecimal("decrease", new BigDecimal("0.05"));
            int numberOfIterations = params.getAsInt("iterations", 5);
            int threads = params.getAsInt("threads", 6);
            File columnsFile = params.getAsFile("columns", "expanded-columns.txt.gz");     
            try {
                new D4().expandColumns(
                        new EQIndex(eqFile),
                        new SignatureBlocksReader(signatureFile).read(),
                        trimmer,
                        expandThreshold,
                        numberOfIterations,
                        decreaseFactor,
                        threads,
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
                                "<string> [default: " + TrimmerType.CENTRIST.toString() + "]"
                        ),
                        new Parameter("threads", "<int> [default: 6]"),
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
            TrimmerType trimmer = TrimmerType
                .valueOf(params.getAsString("trimmer", TrimmerType.CENTRIST.toString()));
            int threads = params.getAsInt("threads", 6);
            File localDomainFile = params.getAsFile("localdomains", "local-domains.txt.gz");
            try {
                new D4().localDomains(
                        new EQIndex(eqFile),
                        columnsFile,
                        new SignatureBlocksReader(signatureFile).read(),
                        trimmer,
                        threads,
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
            File strongDomainFile = params.getAsFile("strongdomains", "strong-domains.txt.gz");
            try {
                new D4().strongDomains(
                        new EQIndex(eqFile),
                        localDomainFile,
                        domainOverlapConstraint,
                        supportFraction,
                        threads,
                        new TelemetryPrinter(),
                        strongDomainFile
                );
            } catch (java.lang.InterruptedException | java.io.IOException ex) {
                LOGGER.log(Level.SEVERE, "STRONG DOMAINS", ex);
                System.exit(-1);
            }
        } else if (command.equals(STEP_EXPORT)) {
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
                        new Parameter("output", "<direcory> [default: 'domains']"),
                    },
                    args
            );
            File eqFile = params.getAsFile("eqs", "compressed-term-index.txt.gz");
            File termFile = params.getAsFile("terms", "term-index.txt.gz");
            File columnsFile = params.getAsFile("columns", "columns.tsv");
            File domainsFile = params.getAsFile("domains", "strong-domains.txt.gz");
            File outputDir = params.getAsFile("output", "domains");
            try {
                new DomainsExportWriter().run(
                        new EQIndex(eqFile),
                        new TermIndexReader(termFile),
                        new DomainReader(domainsFile).read().toList(),
                        columnsFile,
                        outputDir
                );
            } catch (java.io.IOException ex) {
                LOGGER.log(Level.SEVERE, "STRONG DOMAINS", ex);
                System.exit(-1);
            }
        } else {
            System.out.println(UNKNOWN);
            System.exit(-1);
        }
    }
}
