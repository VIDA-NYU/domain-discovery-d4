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
package org.opendata.core.value;

/**
 * Default transformer for column values. Replace successive white spaces with
 * a single space character.
 * 
 * @author Heiko Mueller <heiko.mueller@nyu.edu>
 */
public class DefaultValueTransformer implements ValueTransformer {

    @Override
    public String transform(String value) {

        return value.toUpperCase().replaceAll("\\s+", " ").trim();
    }
}
