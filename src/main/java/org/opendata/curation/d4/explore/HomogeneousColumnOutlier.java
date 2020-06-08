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
package org.opendata.curation.d4.explore;

import java.io.File;
import java.io.PrintWriter;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.opendata.core.io.EntitySetReader;
import org.opendata.core.io.FileSystem;
import org.opendata.core.metric.Precision;
import org.opendata.core.metric.Recall;
import org.opendata.core.object.IdentifiableObject;
import org.opendata.core.set.EntitySet;
import org.opendata.core.set.HashIDSet;
import org.opendata.core.set.IDSet;
import org.opendata.core.util.StringHelper;
import org.opendata.core.util.count.IdentifiableCount;
import org.opendata.core.util.count.IdentifiableCounterSet;
import org.opendata.core.util.count.NamedCount;
import org.opendata.curation.d4.domain.Domain;
import org.opendata.curation.d4.domain.DomainReader;
import org.opendata.db.Database;
import org.opendata.db.column.Column;
import org.opendata.db.column.ColumnNameReader;
import org.opendata.db.eq.EQ;
import org.opendata.db.eq.EQIndex;

/**
 * Generator for local domains using undirected graphs. Each connected component
 * in the graph generated from the robust signatures of the column elements 
 * represents a local domain.
 * 
 * Uses a concurrent queue to distribute columns across workers. Relies
 * 
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class HomogeneousColumnOutlier {
    
	private class Synonym implements Comparable<Synonym> {
		
		private int _count;
		private final int _origNode;
		private final int _replaceNode;
		
		public Synonym(int origNode, int replaceNode) {
			
			_origNode = origNode;
			_replaceNode = replaceNode;
			_count = 1;
		}

		@Override
		public int compareTo(Synonym syn) {

			return Integer.compare(_count, syn.count());
		}

		public int count() {
			
			return _count;
		}
		
		public void inc() {
			
			_count++;
		}
		
		public String key() {
			
			return _origNode + "#" + _replaceNode;
		}
		
		public int origNode() {
			
			return _origNode;
		}
		
		public int replaceNode() {
			
			return _replaceNode;
		}
	}
		
	private class DomainOutliers implements Comparable<DomainOutliers>, IdentifiableObject {
	
		private final HashIDSet _columns;
		private final Domain _domain;
		private final IdentifiableCounterSet _expandCandidates;
		private final IdentifiableCounterSet _nullCandidates;
		private final IdentifiableCounterSet _synonymCandidates;
		private final HashMap<String, Synonym> _synonyms;
		
		public DomainOutliers(Domain domain) {
			
			_domain = domain;
			
			_columns = new HashIDSet(domain.columns());
			_expandCandidates = new IdentifiableCounterSet();
			_nullCandidates = new IdentifiableCounterSet();
			_synonymCandidates = new IdentifiableCounterSet();
			_synonyms = new HashMap<>();
		}
		
		public void addSynonym(int origNode, int replaceNode) {
			
			Synonym syn = new Synonym(origNode, replaceNode);
			if (_synonyms.containsKey(syn.key())) {
				_synonyms.get(syn.key()).inc();
			} else {
				_synonyms.put(syn.key(), syn);
			}
			this.synonymCandidates().inc(replaceNode);
		}
		
		public HashIDSet columns() {
			
			return _columns;
		}

		@Override
		public int compareTo(DomainOutliers domain) {

			return Integer.compare(_columns.length(), domain.columns().length());
		}
		
		public IdentifiableCounterSet expandCandidates() {
			
			return _expandCandidates;
		}
		
		public int id() {
			
			return _domain.id();
		}
		
		public IDSet nodes() {
			
			return new HashIDSet(_domain.nodes());
		}
		
		public IdentifiableCounterSet nullCandidates() {
			
			return _nullCandidates;
		}
		
		public IdentifiableCounterSet synonymCandidates() {
			
			return _synonymCandidates;
		}
		
		public List<Synonym> synonymMapping() {
			
			return new ArrayList<>(_synonyms.values());
		}
	}
	
	private String printDomain(
			EQIndex eqIndex,
			EntitySet terms,
			Database db,
			EntitySet columnNames,
			DomainOutliers domain,
			PrintWriter out
	) {
		String headline = "Domain " + domain.id();
		out.println(headline);
		out.println(StringHelper.repeat("=", headline.length()));
		out.println();
		
		List<String> values = new ArrayList<>();
    	for (int nodeId : domain.nodes()) {
    		EQ node = eqIndex.get(nodeId);
    		for (int termId : node.terms()) {
    			values.add(terms.get(termId).name());
    		}
    	}
    	Collections.sort(values);
    	for (String val : values) {
    		out.println(val);
    	}
    	
    	HashMap<String, NamedCount> colCounts = new HashMap<>();
    	for (int columnId : domain.columns()) {
    		String name = columnNames.get(columnId).name();
    		if (!colCounts.containsKey(name)) {
    			colCounts.put(name, new NamedCount(name, 1));
    		} else {
    			colCounts.get(name).inc();
    		}
    	}
    	List<NamedCount> columns = new ArrayList<>(colCounts.values());
    	Collections.sort(columns, Collections.reverseOrder());

    	out.println();
		headline = "Columns";
		out.println(headline);
		out.println(StringHelper.repeat("=", headline.length()));
		out.println();
		for (NamedCount col : columns) {
			out.println(col.count() + "\t" + col.name());
		}
		
    	if (domain.nullCandidates().size() > 0) {
    		out.println();
    		this.printOutliers(eqIndex, terms, "Null Candidates", domain.nullCandidates(), out);
    	}
    	
    	if (domain.synonymCandidates().size() > 0) {
    		out.println();
    		List<Synonym> synonyms = domain.synonymMapping();
    		Collections.sort(synonyms, Collections.reverseOrder());
    		out.println("Synonym Candidates");
    		out.println(StringHelper.repeat("=", new String("Synonym Candidates").length()));
    		out.println();
    		for (Synonym synonym : synonyms) {
    			List<String> termsOrig = new ArrayList<>();
    			for (int termId : eqIndex.get(synonym.origNode()).terms()) {
    				termsOrig.add(terms.get(termId).name());
    			}
    			Collections.sort(termsOrig);
    			List<String> termsReplace = new ArrayList<>();
    			for (int termId : eqIndex.get(synonym.replaceNode()).terms()) {
    				termsReplace.add(terms.get(termId).name());
    			}
    			Collections.sort(termsReplace);
    			out.println(synonym.count() + "\t" + termsReplace.get(0) + "\t->\t" + termsOrig.get(0));
    			for (int iTerm = 1; iTerm < Math.max(termsOrig.size(), termsReplace.size()); iTerm++) {
    				String termReplace = "";
    				if (iTerm < termsReplace.size()) {
    					termReplace = termsReplace.get(iTerm);
    				}
    				String termOrig = "";
    				if (iTerm < termsOrig.size()) {
    					termOrig = termsOrig.get(iTerm);
    				}
        			out.println("\t" + termReplace + "\t\t" + termOrig);
    			}
    			out.println();
    		}
    	}
    	
    	if (domain.expandCandidates().size() > 0) {
    		out.println();
    		this.printOutliers(eqIndex, terms, "Expansion Candidates", domain.expandCandidates(), out);
    	}

    	out.println();
		headline = "Other columns that contain domain terms";
		out.println(headline);
		out.println(StringHelper.repeat("=", headline.length()));
		out.println();
		List<NamedCount> colCands = new ArrayList<>();
		
		for (Column column : db.columns()) {
			if (!domain.columns().contains(column.id())) {
				int overlap = domain.nodes().overlap(column);
				if (overlap > 0) {
					String key = Integer.toString(column.id());
					colCands.add(new NamedCount(key, overlap));
				}
			}
		}
		Collections.sort(colCands, Collections.reverseOrder());
		for (NamedCount colCand : colCands) {
			int columnId = Integer.parseInt(colCand.name());
			Column column = db.columns().get(columnId);
			int overlap = colCand.count();
			Recall recall = new Recall(overlap, domain.nodes().length());
			Precision precision = new Precision(overlap, column.length());
			out.println(column.id() + "\t" + recall + "\t" + precision + "\t" + columnNames.get(column.id()).name());
		}
		
		return columns.get(0).name();
	}
	
    private void printHeader(String headline, PrintWriter out) {
        
        out.println("<!DOCTYPE html>");
        out.println("<html>");
        out.println("<meta charset=\"utf-8\">");
        out.println("<title>" + headline + "</title>");
        out.println("<link href=\"./styles.css\" rel=\"stylesheet\">");
        out.println("</html>");
        out.println("<body>");
        
        out.println("<h1>" + headline + "</h1>");
    }
	
	private void printOutliers(
			EQIndex eqIndex,
			EntitySet terms,
			String headline,
			IdentifiableCounterSet outliers,
			PrintWriter out
	) {
		out.println(headline);
		out.println(StringHelper.repeat("=", headline.length()));
		out.println();
		
    	for (IdentifiableCount outlier : outliers.toSortedList(true)) {
    		EQ node = eqIndex.get(outlier.id());
    		String count = Integer.toString(outlier.count());
    		for (int termId : node.terms()) {
    			out.println(count + "\t" + terms.get(termId).name());
    			count = "";
    		}
    	}
	}
	
    public void run(
            EQIndex eqIndex,
            EntitySetReader termReader,
            EntitySet columnNames,
            List<Domain> domains,
            File outputDir
    ) throws java.io.IOException {

    	FileSystem.createFolder(outputDir);
    	
    	Database db = new Database(eqIndex);
    	
    	IdentifiableCounterSet nullCandCounts = new IdentifiableCounterSet();
    	IdentifiableCounterSet synonymCandCounts = new IdentifiableCounterSet();
    	IdentifiableCounterSet totalCounts = new IdentifiableCounterSet();
    	
    	HashIDSet termFilter = new HashIDSet();
    	
		List<DomainOutliers> domainInfo = new ArrayList<>();
    	for (Domain domain : domains) {
			DomainOutliers domainOutliers = new DomainOutliers(domain);
			for (Column column : db.columns()) {
	    		if (column.length() <= 1) {
	    			continue;
	    		}
				if (!domain.columns().contains(column.id())) {
					int overlap = column.overlap(domain);
					if (overlap == column.length() - 1) {
	    				IDSet diff = column.difference(domain);
	    				if (diff.length() == 1) {
	    					int nodeId = diff.first();
	    					if (overlap < domain.length() ) {
	    						if (overlap == (domain.length() - 1)) {
	    							int origNode = domain.difference(column).first();
	    							domainOutliers.addSynonym(origNode, nodeId);
	    							synonymCandCounts.inc(nodeId);
	    						} else {
	    							domainOutliers.expandCandidates().inc(nodeId);
	    						}
	    					} else {
	    						domainOutliers.nullCandidates().inc(nodeId);
	    						nullCandCounts.inc(nodeId);
	    					}
	    					totalCounts.inc(nodeId);
	    					domainOutliers.columns().add(column.id());
	    				}
					}
				}
	    	}
			termFilter.add(domain);
			domainInfo.add(domainOutliers);
		}
		
		termFilter.add(totalCounts.keys());
    	EntitySet terms = termReader.readEntities(eqIndex, termFilter);
    	
    	File totalOutlierFile = FileSystem.joinPath(outputDir, "outliers.txt");
    	try (PrintWriter out = FileSystem.openPrintWriter(totalOutlierFile)) {
    		this.printOutliers(eqIndex, terms, "Homogeneous Column Outlier", totalCounts, out);
    	}
    	
    	File nullCandFile = FileSystem.joinPath(outputDir, "null-candidates.txt");
    	try (PrintWriter out = FileSystem.openPrintWriter(nullCandFile)) {
    		this.printOutliers(eqIndex, terms, "Null Candidates", nullCandCounts, out);
    	}
    	
    	File synonymCandFile = FileSystem.joinPath(outputDir, "synonym-candidates.txt");
    	try (PrintWriter out = FileSystem.openPrintWriter(synonymCandFile)) {
    		this.printOutliers(eqIndex, terms, "Synonym Candidates", synonymCandCounts, out);
    	}
    	
    	Collections.sort(domainInfo, Collections.reverseOrder());
    	
    	File indexFile = FileSystem.joinPath(outputDir,  "index.html");
    	try (PrintWriter out = FileSystem.openPrintWriter(indexFile)) {
    		this.printHeader("Homogeneous Column Domains", out);
    		out.println("<p><a href=\"./null-candidates.txt\">Null Candidates</a></p>");
    		out.println("<p><a href=\"./synonym-candidates.txt\">Synonym Candidates</a></p>");
            out.println("<table>");
            out.println(
                    "<thead><tr>" +
                        "<th>Most Frequent Column Name</th>" +
                        "<th class=\"num-col\">#EQs</th>" +
                        "<th class=\"num-col\">#Columns</th>" +
                        "<th class=\"num-col\">#Null Candidates</th>" +
                        "<th class=\"num-col\">#Synonym Candidates</th>" +
                    "</tr></thead>"
            );
            out.println("<tbody>");
            
            HashMap<Integer, String> domainNames = new HashMap<>();
    		for (DomainOutliers domain : domainInfo) {
    			String filename = "domain." + domain.id() + ".txt";
    			File domainFile = FileSystem.joinPath(outputDir, filename);
    			String name = null;
    			try (PrintWriter domOut = FileSystem.openPrintWriter(domainFile)) {
    				name = this.printDomain(eqIndex, terms, db, columnNames, domain, domOut);
    			}
    			domainNames.put(domain.id(), name);
    			out.println(
    					"<tr><td><a href=\"./" + filename + "\">" +
    					name.replaceAll("\"", "").trim() + "</a></td><td class=\"num\">" +
						domain.nodes().length() + "</td><td class=\"num\">"+
						domain.columns().length() + "</td><td class=\"num\">" +
						domain.nullCandidates().size() + "</td><td class=\"num\">" +
						domain.synonymCandidates().size() + "</td></tr>"
				);
    		}
	        out.println("</tbody>");
	        
	        for (Column column : db.columns()) {
	        	ArrayList<String> doms = new ArrayList<>();
	        	for (DomainOutliers domain : domainInfo) {
	        		if (!domain.columns().contains(column.id())) {
	        			Recall recall = new Recall(column.overlap(domain.nodes()), domain.nodes().length());
	        			if (recall.value().compareTo(new BigDecimal(0.5)) > 0) {
	        				doms.add(domainNames.get(domain.id()));
	        			}
	        		}
	        	}
	        	if (doms.size() > 1) {
	        		System.out.println(columnNames.get(column.id()).name());
	        		for (String domName : doms) {
	        			System.out.println("\t" + domName);
	        		}
	        	}
	        }
    	}
    }
    
    private static final String COMMAND =
            "Usage\n" +
            "  <eq-file>\n" +
    		"  <term-index>\n" +
            "  <columns-file>\n" +
            "  <domains-file>\n" +
            "  <output-dir>";
    
    private static final Logger LOGGER = Logger
            .getLogger(HomogeneousColumnOutlier.class.getName());
    
    public static void main(String[] args) {
        
        if (args.length != 5) {
            System.out.println(COMMAND);
            System.exit(-1);
        }
        
        File eqFile = new File(args[0]);
        File termFile = new File(args[1]);
        File columnsFile = new File(args[2]);
        File domainFile = new File(args[3]);
        File outputDir = new File(args[4]);
        
		try {
            new HomogeneousColumnOutlier().run(
            		new EQIndex(eqFile),
            		new EntitySetReader(termFile),
            		new ColumnNameReader(columnsFile).read(),
                    new DomainReader(domainFile).read().toList(),
                    outputDir
            );
        } catch (java.io.IOException ex) {
            LOGGER.log(Level.SEVERE, "RUN", ex);
            System.exit(-1);
        }
    }
}
