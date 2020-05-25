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
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.opendata.curation.d4.column.ExpandedColumnIndex;
import org.opendata.curation.d4.column.ExpandedColumnReader;
import org.opendata.curation.d4.column.ExpandedColumnStats;
import org.opendata.curation.d4.column.NoExpandColumnsWriter;
import org.opendata.curation.d4.domain.DomainReader;
import org.opendata.curation.d4.domain.DomainSetStatsPrinter;
import org.opendata.curation.d4.domain.DomainWriter;
import org.opendata.curation.d4.domain.ParallelLocalDomainGenerator;
import org.opendata.curation.d4.domain.StrongDomainGenerator;
import org.opendata.curation.d4.signature.SignatureBlocksGenerator;
import org.opendata.curation.d4.signature.SignatureBlocksStats;
import org.opendata.curation.d4.signature.SignatureBlocksStream;
import org.opendata.core.constraint.Threshold;
import org.opendata.core.io.FileSystem;
import org.opendata.core.set.IdentifiableObjectSet;
import org.opendata.curation.d4.domain.Domain;
import org.opendata.curation.d4.domain.StrongDomain;
import org.opendata.curation.d4.domain.StrongDomainReader;
import org.opendata.curation.d4.domain.StrongDomainWriter;
import org.opendata.curation.d4.signature.SignatureBlocksReader;
import org.opendata.curation.d4.signature.SignatureBlocksWriter;
import org.opendata.db.eq.EQIndex;

/**
 * Generate strong domains for a given set of equivalence classes without
 * column expansion.
 * 
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class NoExpandDomainGenerator {
    
    private File _columnFile;
    private File _localDomainFile;
    private boolean _overwrite;
    private File _signatureFile;
    private File _strongDomainFile;

    
    public NoExpandDomainGenerator(File outputDir, boolean overwrite) {
		
    	_overwrite = overwrite;

    	_signatureFile = FileSystem.joinPath(
        		outputDir,
        		"signatures.txt.gz"
		);
        _columnFile = FileSystem.joinPath(
    			outputDir,
    			"columns.NO-EXPAND.txt.gz"
    	);
        _localDomainFile = FileSystem.joinPath(
        		outputDir,
        		"local-domains.NO-EXPAND.txt.gz"
        );
        _strongDomainFile = FileSystem.joinPath(
        		outputDir,
        		"strong-domains.NO-EXPAND.txt.gz"
        );
	}
	
    public File columnFile() {
    	
    	return _columnFile;
    }
    
	private void expandColumns(
    		EQIndex nodeIndex,
    		File outputFile
    ) throws java.io.IOException {
        
        System.out.println("\n-- COLUMN EXPANSION\n");
        
        if ((_overwrite) || (!outputFile.exists())) {
        	new NoExpandColumnsWriter().run(nodeIndex, outputFile);        
	        ExpandedColumnStats colStats = new ExpandedColumnStats();
	        new ExpandedColumnReader(outputFile).stream(colStats);
	        colStats.print();
        }
    }
    
    private void localDomains(
            EQIndex nodeIndex,
            File columnsFile,
            SignatureBlocksStream signatures,
            String trimmer,
            int threads,
            File outputFile
    ) throws java.io.IOException {
        
        System.out.println("\n-- LOCAL DOMAINS\n");

        if ((_overwrite) || (!outputFile.exists())) {
	        ExpandedColumnIndex columnIndex = new ExpandedColumnIndex();
	        new ExpandedColumnReader(columnsFile).stream(columnIndex);
	        new ParallelLocalDomainGenerator().run(
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
    }
    
    public File localDomainFile() {
    	
    	return _localDomainFile;
    }
    
    private void signatures(
            EQIndex nodeIndex,
            int threads,
            File outputFile
    ) throws java.lang.InterruptedException, java.io.IOException {
        
        System.out.println("\n-- SIGNATURE BLOCKS\n");
        
        if ((_overwrite) || (!outputFile.exists())) {
	        new SignatureBlocksGenerator().runWithMaxDrop(
	                nodeIndex,
	                new ConcurrentLinkedQueue<>(nodeIndex.keys().toList()),
	                false,
	                true,
	                threads,
	                new SignatureBlocksWriter(outputFile)
	        );
	        SignatureBlocksStats sigStats = new SignatureBlocksStats();
	        new SignatureBlocksReader(outputFile).stream(sigStats);
	        sigStats.print();
        }        
    }
    
    public File signatureFile() {
    	
    	return _signatureFile;
    }
    
    private void strongDomains(
            EQIndex nodeIndex,
            File localDomainFile,
            Threshold domainOverlapConstraint,
            BigDecimal supportFraction,
            int threads,
            File outputFile
    ) throws java.lang.InterruptedException, java.io.IOException {
        
        System.out.println("\n-- STRONG DOMAINS\n");

        if ((_overwrite) || (!outputFile.exists())) {
	        IdentifiableObjectSet<Domain> localDomains;
	        localDomains = new DomainReader(localDomainFile).read();
	        new StrongDomainGenerator().run(
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
    }
    
    public File strongDomainFile() {
    	
    	return _strongDomainFile;
    }

    public void run(
    		File eqFile,
    		int threads
    ) throws java.lang.InterruptedException, java.io.IOException {
    	
    	EQIndex nodeIndex = new EQIndex(eqFile);
    	this.signatures(nodeIndex, threads, _signatureFile);
    	this.expandColumns(nodeIndex, _columnFile);
    	this.localDomains(
    			nodeIndex,
    			_columnFile,
    			new SignatureBlocksReader(_signatureFile),
    			TRIMMER,
    			threads,
    			_localDomainFile
    	);
    	this.strongDomains(
    			nodeIndex,
    			_localDomainFile,
    			Threshold.getConstraint("GT0.5"),
    			new BigDecimal("0.25"),
    			threads,
    			_strongDomainFile
    	);
    }
    
    private static final String COMMAND =
            "Usage:\n" +
            "  <eq-file>\n" +
            "  <threads>\n" +
            "  <output-dir>";
    
    private static final Logger LOGGER = Logger
    		.getLogger(NoExpandDomainGenerator.class.getName());
    
    private static final String TRIMMER = "CENTRIST:GT0.001";
    
    public static void main(String[] args) {
        
        System.out.println(Constants.NAME + " No-Expansion Domain Generator - Version (" + Constants.VERSION + ")\n");

        if (args.length != 3) {
            System.out.println(COMMAND);
            System.exit(-1);
        }
        
        File eqFile = new File(args[0]);
        int threads = Integer.parseInt(args[1]);
        File outputDir = new File(args[2]);
        
    	try {
    		new NoExpandDomainGenerator(outputDir, true).run(eqFile,  threads);
    	} catch (java.lang.InterruptedException | java.io.IOException ex) {
        	LOGGER.log(Level.SEVERE, "RUN", ex);
        	System.exit(-1);
        }
    }
}
