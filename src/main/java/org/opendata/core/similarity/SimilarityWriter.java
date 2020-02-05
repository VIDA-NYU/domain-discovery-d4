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
package org.opendata.core.similarity;

import java.io.PrintWriter;
import java.math.BigDecimal;
import java.math.RoundingMode;
import org.opendata.core.io.SynchronizedWriter;
import org.opendata.core.object.IdentifiableObject;

/**
 * Write similarity between pairs of objects to file.
 * 
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class SimilarityWriter implements ObjectSimilarityConsumer {

    public static final int DEFAULT_SCALE = 8;
    
    private final SynchronizedWriter _out;
    private final int _scale;
    
    public SimilarityWriter(SynchronizedWriter out, int scale) {
        
        _out = out;
        _scale = scale;
    }
    
    public SimilarityWriter(PrintWriter out, int scale) {
        
        this(new SynchronizedWriter(out), scale);
    }
    
    public SimilarityWriter(SynchronizedWriter out) {
        
        this(out, DEFAULT_SCALE);
    }
    
    public SimilarityWriter(PrintWriter out) {
        
        this(new SynchronizedWriter(out), DEFAULT_SCALE);
    }
    
    @Override
    public void consume(IdentifiableObject obj1, IdentifiableObject obj2, BigDecimal sim) {

        _out.write(
            obj1.id() + "\t" +
            obj2.id() + "\t" +
            sim.setScale(_scale, RoundingMode.HALF_DOWN).toPlainString()
        );
    }
}
