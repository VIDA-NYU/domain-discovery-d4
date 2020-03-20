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
package org.opendata.core.query.json;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonToken;
import java.io.File;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.opendata.core.io.FileSystem;

/**
 *
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class JsonQuery {
    
    private final File _database;
    private final String _targetPath;
    
    public JsonQuery(File database, String targetPath) {
        
        _database = database;
        if (targetPath.startsWith("/")) {
            _targetPath = targetPath.substring(1);
        } else {
            _targetPath = targetPath;
        }
    }

    public JsonQuery(File database) {
        
        this(database, "");
    }
    
    private void addPath(
            Entry<String, JsonElement> entry,
            String prefix,
            HashSet<String> schema
    ) {

        String path = prefix + "/" + entry.getKey();
        if (!schema.contains(path)) {
            schema.add(path);
        }
        
        JsonElement el = entry.getValue();
        if (el.isJsonObject()) {
            for (Entry<String, JsonElement> child : el.getAsJsonObject().entrySet()) {
                this.addPath(child, path, schema);
            }
        }
    }

    public List<ResultTuple> executeQuery(
            SelectClause select,
            boolean noNullValues
    ) throws java.io.IOException {

        try (JsonReader reader = new JsonReader(
            new InputStreamReader(FileSystem.openFile(_database)))
        ) {
            if (_targetPath.equals("")) {
                return this.filter(reader, select, noNullValues);
            } else {
                reader.beginObject();
                if (reader.nextName().equals(_targetPath)) {
                    return this.filter(reader, select, noNullValues);
                } else {
                    reader.skipValue();
                }
                reader.endObject();
                return new ArrayList<>();
            }
        }
    }

    public List<ResultTuple> executeQuery(SelectClause select) throws java.io.IOException {
        
        return this.executeQuery(select, false);
    }
    
    private List<ResultTuple> filter(
            JsonReader reader,
            SelectClause select,
            boolean noNullValues
    ) throws java.io.IOException {

        ArrayList<ResultTuple> result = new ArrayList<>();
       
        reader.beginArray();
        while (reader.hasNext()) {
            JsonObject doc = new JsonParser().parse(reader).getAsJsonObject();
            JsonElement[] tuple = new JsonElement[select.size()];
            boolean hasNull = false;
            for (int iCol = 0; iCol < select.size(); iCol++) {
                JsonElement el = select.get(iCol).eval(doc);
                if (el != null) {
                    tuple[iCol] = el;
                } else {
                    tuple[iCol] = null;
                    hasNull = true;
                }
            }
            if ((!hasNull) || (!noNullValues)) {
                result.add(new ResultTuple(tuple, select.schema()));
            }
        }
        reader.endArray();
        
        return result;
    }
    
    public void schema(PrintWriter out) throws java.io.IOException {
        
        JsonParser parser = new JsonParser();
                
        try (InputStream is = FileSystem.openFile(_database)) {
            JsonReader reader = new JsonReader(new InputStreamReader(is));
            String root;
            if (!reader.hasNext()) {
                return;
            }
            JsonToken firstToken = reader.peek();
            if (JsonToken.BEGIN_OBJECT.equals(firstToken)) {
                reader.beginObject();
                root = reader.nextName();
            } else {
                root = "";
            }
            HashSet<String> schema = new HashSet();
            reader.beginArray();
            while (reader.hasNext()) {
                JsonObject doc = parser.parse(reader).getAsJsonObject();
                for (Map.Entry<String, JsonElement> entry : doc.entrySet()) {
                    this.addPath(entry, "", schema);
                }
            }
            reader.endArray();
            List<String> paths = new ArrayList<>(schema);
            Collections.sort(paths);
            for (String path : paths) {
                out.println("\t" + path);
            }
            if (JsonToken.BEGIN_OBJECT.equals(firstToken)) {
                reader.endObject();
            }
        }
    }
    
    private static final String COMMAND = 
            "Usage:\n" +
            "  <database-file>" +
            "  [-s | {-t <target-path>} <path-1>, ...]";
    
    private static final Logger LOGGER = Logger
            .getLogger(JsonQuery.class.getName());
    
    public static void main(String[] args) {
        
        if (args.length < 2) {
            System.out.println(COMMAND);
            System.exit(-1);
        }
        
        File databaseFile = new File(args[0]);
        
        try (PrintWriter out = new PrintWriter(System.out)) {
            if ((args.length == 2) && (args[1].equals("-s"))) {
                new JsonQuery(databaseFile).schema(out);
            } else if (args.length >= 2) {
                int offset;
                JsonQuery db;
                if (args[1].equals("-t")) {
                    if (args.length <= 3) {
                        System.out.println(COMMAND);
                        System.exit(-1);
                    }
                    db = new JsonQuery(databaseFile, args[2]);
                    offset = 3;
                } else {
                    db = new JsonQuery(databaseFile);
                    offset = 1;
                }
                SelectClause select = new SelectClause();
                for (int iArg = offset; iArg < args.length; iArg++) {
                    select.add(args[iArg], new JQuery(args[iArg]));
                }
                for (ResultTuple tuple : db.executeQuery(select)) {
                    out.println(tuple.join("\t"));
                }
            } else {
                System.out.println(COMMAND);
                System.exit(-1);
            }
        } catch (java.io.IOException ex) {
            LOGGER.log(Level.SEVERE, "RUN", ex);
            System.exit(-1);
        }
    }
}
