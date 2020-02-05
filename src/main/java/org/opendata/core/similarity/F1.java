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

import java.math.BigDecimal;
import org.opendata.core.util.FormatedBigDecimal;
import org.opendata.core.util.MathHelper;

/**
 * BigDecimal for precision of a set from another.
 * 
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class F1 extends FormatedBigDecimal implements Comparable<F1> {
    
    public F1(Precision precision, Recall recall, int scale) {
        
        super(MathHelper.f1(precision.value(), recall.value()), scale);
    }
    
    public F1(Precision precision, Recall recall) {
        
        super(MathHelper.f1(precision.value(), recall.value()));
    }
    
    public F1() {
        
        super(BigDecimal.ZERO);
    }

    @Override
    public int compareTo(F1 f) {

        return this.value().compareTo(f.value());
    }
}
