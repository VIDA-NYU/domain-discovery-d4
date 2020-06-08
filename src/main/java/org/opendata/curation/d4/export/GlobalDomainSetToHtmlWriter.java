/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.opendata.curation.d4.export;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.stream.JsonReader;
import java.io.File;
import java.io.FileReader;
import java.io.PrintWriter;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.opendata.core.io.FileSystem;
import org.opendata.core.util.FormatedBigDecimal;
import org.opendata.core.util.count.Counter;
import org.opendata.core.util.count.NamedCount;
import org.opendata.curation.d4.Constants;

/**
 * Convert a list of universal domain files to Html.
 * 
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class GlobalDomainSetToHtmlWriter {
    
    private class DomFile implements Comparable<DomFile> {

        private final int _columnCount;
        private final String _domainName;
        private final String _href;
        private final int _sourceCount;
        private final int _termCount;
        
        public DomFile(
                String domainName,
                int sourceCount,
                int columnCount,
                int termCount,
                String href
        ) {
        
            _domainName = domainName;
            _sourceCount = sourceCount;
            _columnCount = columnCount;
            _termCount = termCount;
            _href = href;
        }
        
        public int columnCount() {
            
            return _columnCount;
        }
        
        @Override
        public int compareTo(DomFile f) {
            
            int comp = Integer.compare(f.sourceCount(), this.sourceCount());
            if (comp == 0) {
                comp = Integer.compare(f.columnCount(), this.columnCount());
                if (comp == 0) {
                    comp = this.domainName().compareTo(f.domainName());
                }
            }
            return comp;
        }
        
        public String domainName() {
            
            return _domainName;
        }
        
        public int sourceCount() {
            
            return _sourceCount;
        }
        
        public void writeHtml(int rank, PrintWriter out) {
            
            out.println(
                    "<tr>" +
                    "<td class=\"num\">" + rank + "</td>" +
                    "<td class=\"num\">" + _sourceCount + "</td>" +
                    "<td class=\"num\">" + _columnCount + "</td>" +
                    "<td class=\"num\">" + _termCount + "</td>" +
                    "<td class=\"text\"><a href=\"" + _href + "\">" + _domainName + "</a></td>" +
                    "</tr>"
            );
        }
    }
    
    private static int fileId(File file) {
        
        return Integer.parseInt(file.getName().split("\\.")[0]);
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
    
    public void run(
            File inputDir,
            String headline,
            int maxTermCount,
            File outputDir
    ) throws java.io.IOException {
        
        FileSystem.createFolder(outputDir);
        
        File[] files = inputDir.listFiles();
        
        File indexFile = FileSystem.joinPath(outputDir, "index.html");
        try (PrintWriter out = FileSystem.openPrintWriter(indexFile)) {
            this.printHeader(headline, out);
            out.println("<table>");
            out.println(
                    "<thead><tr>" +
                        "<th class=\"num-col\">#</th>" +
                        "<th class=\"num-col\">Data Sources</th>" +
                        "<th class=\"num-col\">Columns</th>" +
                        "<th class=\"num-col\">Terms</th>" +
                        "<th>Most Frequent Column Name</th>" +
                    "</tr></thead>"
            );
            out.println("<tbody>");
            ArrayList<DomFile> htmlFiles = new ArrayList<>();
            for (File file : files) {
                try (FileReader reader = new FileReader(file)) {
                    JsonObject doc = new JsonParser()
                            .parse(new JsonReader(reader))
                            .getAsJsonObject();
                    String name = doc.get("name").getAsString();
                    JsonArray columns = doc
                            .get("columns")
                            .getAsJsonArray();
                    HashSet<String> dataSources = new HashSet<>();
                    for (int iColumn = 0; iColumn < columns.size(); iColumn++) {
                        String domainName = columns
                                .get(iColumn)
                                .getAsJsonObject()
                                .get("domain")
                                .getAsString();
                        if (!dataSources.contains(domainName)) {
                            dataSources.add(domainName);
                        }
                    }
                    JsonArray termsArray = doc.get("terms").getAsJsonArray();
                    int termCount = 0;
                    for (JsonElement el : termsArray) {
                        termCount += el.getAsJsonArray().size();
                    }
                    String filename = fileId(file) + ".html";
                    File outputFile = FileSystem.joinPath(outputDir, filename);
                    try (PrintWriter outPrinter = FileSystem.openPrintWriter(outputFile)) {
                        String title = "Domain " + name;
                        this.writeFile(doc, headline, title, dataSources, maxTermCount, outPrinter);
                    }
                    String link = "./" + filename;
                    htmlFiles.add(
                            new DomFile(
                                    name,
                                    dataSources.size(),
                                    columns.size(),
                                    termCount,
                                    link
                            )
                    );
                }
            }
            Collections.sort(htmlFiles);
            int rank = 1;
            for (DomFile dFile : htmlFiles) {
                dFile.writeHtml(rank++, out);
            }
            out.println("</tbody></table>");
        }
    }
    
    private void writeFile(
            JsonObject doc,
            String headline,
            String title,
            HashSet<String> domains,
            int maxTermCount,
            PrintWriter out
    ) throws java.io.IOException {
        
        this.printHeader(title, out);
        
        out.println("<p class=\"subtitle\">" + headline + "</p>");
        
        JsonArray columnsArray = doc.get("columns").getAsJsonArray();
        HashMap<String, Counter> columns = new HashMap<>();
        for (JsonElement el : columnsArray) {
            String name = el.getAsJsonObject().get("name").getAsString();
            if (columns.containsKey(name)) {
                columns.get(name).inc();
            } else {
                columns.put(name, new Counter(1));
            }
        }

        JsonArray termsArray = doc.get("terms").getAsJsonArray();
        int termCount = 0;
        for (JsonElement el : termsArray) {
            termCount += el.getAsJsonArray().size();
        }
        
        String mainNav = 
                "<a class=\"nav\" href=\"#data-sources\">Data Sources (" + domains.size() + ")</a>&nbsp;&nbsp;&nbsp;" +
                "<a class=\"nav\" href=\"#columns\">Columns (distinct " + columns.size() + ", total " + columnsArray.size() + ")</a>&nbsp;&nbsp;&nbsp;" +
                "<a class=\"nav\" href=\"#terms\">Terms (" + termCount + ")</a>";
        out.println("<p class=\"main-navbar\">" + mainNav + "</p>");
        
        // Data Sources
        out.println("<h4><a class=\"name\" name=\"data-sources\">Data Sources (" + domains.size() + ")</a></h4>");
        List<String> dataSources = new ArrayList<>(domains);
        Collections.sort(dataSources);
        for (String ds : dataSources) {
            out.println("<p class=\"ds-name\">" + ds + "</p>");
        }
        // Columns
        List<NamedCount> columnNames = new ArrayList<>();
        for (String name : columns.keySet()) {
            columnNames.add(new NamedCount(name, columns.get(name).value()));
        }
        Collections.sort(columnNames);
        Collections.reverse(columnNames);
        out.println("<h4><a class=\"name\" name=\"columns\">Columns (distinct " + columns.size() + ", total " + columnsArray.size() + ")</a></h4>");
        for (NamedCount col : columnNames) {
            out.println(
                    "<p class=\"col-name\">" +
                            col.name().toLowerCase() +
                            " (" + col.count() +
                    ")</p>"
            );
        }
        // Terms
        String css = "s-term";
        out.println("<h4><a class=\"name\" name=\"terms\">Terms (" + termCount + ")</a></h4>");
        String navbar = null;
        for (int iBlock = 0; iBlock < termsArray.size(); iBlock++) {
            JsonArray block = termsArray.get(iBlock).getAsJsonArray();
            String blockTitle = "#" + (iBlock + 1) + " (" + block.size() + ")";
            blockTitle = "<a class=\"nav\" href=\"#blck" + iBlock + "\">" +  blockTitle + "</a>";
            if (iBlock == 0) {
                navbar = blockTitle;
            } else {
                navbar += "&nbsp;&nbsp;&nbsp;" + blockTitle;
            }
        }
        int termOutputCount = 0;
        out.println("<p class=\"navbar\">" + navbar + "</p>");
        for (int iBlock = 0; iBlock < termsArray.size(); iBlock++) {
            JsonArray block = termsArray.get(iBlock).getAsJsonArray();
            out.println("<p class=\"section\"><a class=\"name\" name=\"blck" + iBlock + "\">Block #" + (iBlock + 1) + " - " + block.size() + " term(s)</a></p>");
            for (JsonElement el : block) {
                JsonObject obj = el.getAsJsonObject();
                out.println(this.termToString(obj, css));
                termOutputCount++;
                if (termOutputCount > maxTermCount) {
                    break;
                }
            }
            css = "w-term";
        }
    }
    
    private String termToString(JsonObject obj, String css) {
        
        String term = obj.get("name").getAsString();
        term = term.replaceAll("<", "&lt;").replaceAll(">", "&gt;");
        term = "<span class=\"" + css + "\">" + term + "</span>";
        BigDecimal weight = obj.get("weight").getAsBigDecimal();
        return "<p class=\"term\">" + term + " (" + new FormatedBigDecimal(weight) + ")</p>";
    }
    
    private final static String COMMAND =
            "Usage:\n" +
            "  <input-dir>\n" +
            "  <headline>\n" +
            "  <max-terms-in-output>\n" +
            "  <output-dir>";
    
    private final static Logger LOGGER = Logger
            .getLogger(GlobalDomainSetToHtmlWriter.class.getName());
    
    public static void main(String[] args) {
        
        System.out.println(Constants.NAME + " - Global Domain to Html Writer - Version (" + Constants.VERSION + ")\n");
        
        if (args.length != 4) {
            System.out.println(COMMAND);
            System.exit(-1);
        }
        
        File inputDir = new File(args[0]);
        String headline = args[1];
        int maxTermCount = Integer.parseInt(args[2]);
        File outputDir = new File(args[3]);
        
        try {
            new GlobalDomainSetToHtmlWriter()
                    .run(inputDir, headline, maxTermCount, outputDir);
        } catch (java.io.IOException ex) {
            LOGGER.log(Level.SEVERE, "RUN", ex);
            System.exit(-1);
        }
    }
}
