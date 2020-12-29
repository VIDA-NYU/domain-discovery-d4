Data-Driven Domain Discovery (D4)
=================================

[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0) [![DOI](https://zenodo.org/badge/238518915.svg)](https://zenodo.org/badge/latestdoi/238518915)


## About

D4 implements a data-driven domain discovery approach for collections of related tabular (structured) datasets. Given collection of datasets, D4 outputs  a set of domains discovered from the collection in a holistic fashion, by taking all the data into account.

Similar to word embedding methods such as Word2Vec, D4 gathers contextual information for terms. But unlike these methods which aim to build context for terms in unstructured text, we aim to capture context for terms within columns in a set of tables. The intuition is that terms from the same domain frequently occur together in columns or at least with similar sets of terms.

For more information about D4, please have a look at our paper [Data-Driven Domain Discovery for Structured Datasets](http://www.vldb.org/pvldb/vol13/p953-ota.pdf) at VLDB 2020:

*Masayo Ota, Heiko Müller, Juliana Freire, and Divesh Srivastava*.
**Data-driven domain discovery for structured datasets.**
Proc. VLDB Endow. 13, 7 (March 2020), 953–967.
DOI:https://doi.org/10.14778/3384345.3384346

Note: This repository merges the relevant parts of previously separated repositories [urban-data-core](https://github.com/VIDA-NYU/urban-data-core) and [urban-data-db](https://github.com/VIDA-NYU/urban-data-db).


## Installation

The D4 jar-file can be build using [Apache Maven](https://maven.apache.org/). After cloning the repository, run `mvn clean install` to build the D4 jar-file.

```
git clone git@github.com:VIDA-NYU/domain-discovery-d4.git
cd domain-discovery-d4
mvn clean install
cp target/D4-jar-with-dependencies.jar /home/user/lib/D4.jar
```

## Domain Discovery Pipeline

The D4 algorithm operates on a set of CSV files. All input files are currently expected within a single directory. D4 will consider all files in the directory with suffix `.csv`, `.csv.gz`, `.tsv`, or `.tsv.gz`. Files with suffix `.csv` or `.csv.gz` are expected to be comma-separated files. Files with suffix `.tsv` or `.tsv.gz` are expected to be tab-delimited files. The first line in each file is expected to contain the column header. The `D4.jar` file supports nine different commands:

```
$> java -jar /home/user/lib/D4.jar --help

Usage:
  <command> [

      Data preparation
      ----------------
      columns
      term-index
      eqs

      D4 pipeline
      -----------
      signatures
      expand-columns
      local-domains
      strong-domains

      Alternatives
      ------------
      no-expand
      columns-as-domains

      Explore Results
      ---------------
      export

  ] <args>
```

The first four commands are required for transforming the input datasets into the internal format that is used by D4.

### Data Preparation

**Generate Column Files:** The first command converts a set of CSV files into a set of column files, one file for each column in the dataset collection. The resulting column files are tab-delimited and contain a list of distinct terms for the respective column and the frequencies for individual terms. Each column has a unique identifier. Column metadata (i.e., column name and dataset file) are written to a metadata file. During this step all column values will be converted to upper case (to make the domain-discovery process case-insensitive).

The `cacheSize` parameter specifies the size of the memory cache for each column that is used to generate the list of distinct terms and their counts.

```
$> java -jar  /home/user/lib/D4.jar columns --help
D4 - Data-Driven Domain Discovery - Version (0.28.0)

columns
  --input=<directory> [default: 'tsv']
  --metadata=<file> [default: 'columns.tsv']
  --cacheSize=<int> [default: 1000]
  --verbose=<boolean> [default: true]
  --threads=<int> [default: 6]
  --output=<directory> [default: 'columns']
```

**Create Index of Unique Terms in the Data Collection:** The term index contains a list of unique terms across all columns in the collection. In this step the user has the option to consider only a subset of all columns in the collection (e.g., only those columns that were classified as text columns). The `--textThreshold` parameter specifies the fraction of distinct terms in a column that have to be classified as *text* for the column to be included in the term index..

```
$> java -jar /home/user/lib/D4.jar term-index --help
D4 - Data-Driven Domain Discovery - Version (0.28.0)

term-index
  --input=<directory | file> [default: 'columns']
  --textThreshold=<constraint> [default: 'GT0.5']
  --membuffer=<int> [default: 10000000]
  --validate=<boolean> [default: false]
  --threads=<int> [default: 6]
  --verbose=<boolean> [default: true]
  --output=<file> [default: 'text-columns.txt']
```

**Generate Equivalence Classes:** D4 operates on sets of equivalence classes. Equivalence classes are sets of terms that always occur in the same set of columns.

```
$> java -jar /home/user/lib/D4.jar eqs --help
D4 - Data-Driven Domain Discovery - Version (0.28.0)

eqs
  --input=<file> [default: 'term-index.txt.gz']
  --verbose=<boolean> [default: true]
  --output=<file> [default: 'compressed-term-index.txt.gz']
```

### Domain Discovery

D4 has three main steps: signature generation, column expansion, and domain discovery.

**Robust Signatures:** The first step creates signatures that capture the context of terms taking into account term co-occurrence information over all columns in the collection. These signatures are made robust to noise, heterogeneity, and ambiguity. Robust signatures capture the context of related terms while blending out noise. They are essential to our approach in addressing the challenges of incomplete columns (through column expansion) and term ambiguity in heterogeneous and noisy data.

```
$> java -jar /home/user/lib/D4.jar signatures --help
D4 - Data-Driven Domain Discovery - Version (0.28.0)

signatures
  --eqs=<file> [default: 'compressed-term-index.txt.gz']
  --sim=<str> [default: JI | LOGJI | TF-ICF]
  --robustifier=<str> [default: LIBERAL | COMMON-COLUMN | IGNORE-LAST]
  --fullSignatureConstraint=<boolean> [default: true]
  --ignoreLastDrop=<boolean> [default: false]
  --ignoreMinorDrop=<boolean> [default: true] 
  --threads=<int> [default: 6]
  --verbose=<boolean> [default: true]
  --signatures=<file> [default: 'signatures.txt.gz']
```

The robust signature step starts by computing context signatures for each term. A *context signature* is a vector containing similarities of a term with all other terms in the dataset. Elements in the context signature are sorted in decreasing order of similarity. The `--sim` parameter specifies the similarity function that is used to create the context signature. the following similarity functions are currently supported:

- JI: Jaccard-Index similarity between the sets of columns that two equivalence classes.
- LOGJI: Logarithm of the Jaccard-Index similarity
- TF-ICF: Weighted Jaccard-Index similarity. The weights for each term are computed using a tf-idf-like measure.

For signature robustification the context signature is first divided into blocks of elements based on the idea of consecutive steepest drop , i.e., the maximum difference between consecutive elements in the sorted context signature. The `--ignoreMinorDrop` parameter can be used to avoid splitting the context signature in too many blocks based on irrelevant steepest drops in regions of low variablility. A minor drop is detected if the next steepest drop is smaller than the difference of the elements in the block that preceeds the drop. If the `--ignoreMinorDrop` parameter is `true` all remaining elements will be placed in a single final block if a minor drop occurs.

D4 then prunes all blocks starting from *noisy block* and only retains blocks that occur before that noisy block. There are three different strategies to identify the noisy block (controlled via the `--robustifier` parameter):

- COMMON-COLUMN: The noisy block is the first block where **NOT** all terms in the block occur together in at least one column. The motivation here is that blocks are supposed to represent subsets of domains that a term belongs to. A block that contains terms that never occur to gether in at least one column is likely to contains terms that do not belong to the same domain.
- IGNORE-LAST: Keeps all blocks except the last block. The only exception is if the context signature contains only a single block. This pruning strategy is particularly intended to be used in combination with `--ignireMinorDrop=true`. If a minor drop is detected all remaining elements in the context signature are placed in a single (final) block.
- LIBERAL: Default setting considers the number of terms in each block. The larges block is then considered as the noisy block for pruning.


**Expanded Columns:** The *column expansion* step addresses the challenge of incomplete columns by adding terms to a column that are likely to belong to the same domain as the majority of terms in the column. D4 makes use of robust signatures to expand columns: it adds a term only if it has sufficient support (controlled by `expandThreshold`) from the robust signatures of terms in the column. This leads to high accuracy in domain discovery. We take an iterative approach to column expansion. The idea is that adding a term to a column may provide additional support for other terms to be added as well (controlled by `iterations` and `decrease`).

```
$> java -jar /home/user/lib/D4.jar expand-columns --help
D4 - Data-Driven Domain Discovery - Version (0.28.0)

expand-columns
  --eqs=<file> [default: 'compressed-term-index.txt.gz']
  --signatures=<file> [default: 'signatures.txt.gz']
  --trimmer=<string> [default: CENTRIST] (alternatives: CONSERVATIVE, LIBERAL)
  --expandThreshold=<constraint> [default: 'GT0.25']
  --decrease=<double> [default: 0.05]
  --iterations=<int> [default: 5]
  --threads=<int> [default: 6]
  --verbose=<boolean> [default: true]
  --columns=<file> [default: 'expanded-columns.txt.gz']
```

Valid values for the `--trimmer`parameter are:

- CONSERVATIVE
- CENTRIST
- LIBERAL

**Local Domains:** This step derives from each column a set of domain candidates, called *local domains*. Local domains are clusters of terms in an (expanded) column that are likely to belong to the same type.

```
$> java -jar /home/user/lib/D4.jar local-domains --help
D4 - Data-Driven Domain Discovery - Version (0.28.0)

local-domains
  --eqs=<file> [default: 'compressed-term-index.txt.gz']
  --columns=<file> [default: 'expanded-columns.txt.gz']
  --signatures=<file> [default: 'signatures.txt.gz']
  --trimmer=<string> [default: CENTRIST]
  --threads=<int> [default: 6]
  --verbose=<boolean> [default: true]
  --localdomains=<file> [default: 'local-domains.txt.gz']
```

**Strong Domains:** In this step, D4 applies a data-driven approach to narrow down the set of local domains and create a smaller set of *strong domains* to be presented to the user. Strong domain are sets of local domains that provide support for each other based on overlap.

```
$> java -jar /home/user/lib/D4.jar strong-domains --help
D4 - Data-Driven Domain Discovery - Version (0.28.0)

strong-domains
  --eqs=<file> [default: 'compressed-term-index.txt.gz']
  --localdomains=<file> [default: 'local-domains.txt.gz']
  --domainOverlap=<constraint> [default: 'GT0.5']
  --supportFraction=<double> [default: 0.25]
  --threads=<int> [default: 6]
  --verbose=<boolean> [default: true]
  --strongdomains=<file> [default: 'strong-domains.txt.gz']
```


#### Alternatives

**No expansion:** Use the `no-expand` option when discovering local domains on the original dataset columns (without expansion). This option will output a columns file in the same format as the `expand-columns` step that can be used as input for the `local-domains` step.

```
$> java -jar /home/user/lib/D4.jar no-expand --help
D4 - Data-Driven Domain Discovery - Version (0.30.1)

no-expand
  --eqs=<file> [default: 'compressed-term-index.txt.gz']
  --verbose=<boolean> [default: true]
  --columns=<file> [default: 'expanded-columns.txt.gz']
```


**Whole column as domain:** Instead of discovering local domains within (expanded) columns there is now an option to treat each unique (expanded) column as a local domain.

```
$> java -jar /home/user/lib/D4.jar columns-as-domains --help
D4 - Data-Driven Domain Discovery - Version (0.30.1)

columns-as-domains
  --eqs=<file> [default: 'compressed-term-index.txt.gz']
  --columns=<file> [default: 'expanded-columns.txt.gz']
  --verbose=<boolean> [default: true]
  --localdomains=<file> [default: 'local-domains.txt.gz']
```


### Explore Results

**Export Domains:** The discovered strong domains can be exported as JSON files for exploration. For each domain a separate file will be created in the output directory. The `--sampleSize` parameter controls the maximum number of terms that are included in the result for each equivalence class.

```
$> java -jar /home/user/lib/D4.jar export --help
D4 - Data-Driven Domain Discovery - Version (0.28.0)

export-domains
  --eqs=<file> [default: 'compressed-term-index.txt.gz']
  --terms=<file> [default: 'term-index.txt.gz']
  --columns=<file> [default: 'columns.tsv']
  --domains=<file> [default: 'strong-domains.txt.gz']
  --sampleSize=<int> [default: 100]
  --writePrimary=<boolean> [default: true]
  --output=<direcory> [default: 'domains']
```

Each output file contains (i) a domain name (derived from frequent tokens in the domain columns), (ii) the list of columns to which the strong domain is assigned, and (iii) the list of terms in the local domains that for the strong domain. Each term in the strong domain is assigned a weight which is computed based on the number of local domain that contain the term. Terms in the strong domain are grouped into blocks based on their weights. An example output file is shown below:

```
{
  "name": "source",
  "columns": [
    {
      "id": 2852,
      "name": "source",
      "dataset": "sftu-nd43"
    },
    {
      "id": 3985,
      "name": "source",
      "dataset": "9dsr-3f97"
    }
  ],
  "terms": [
    [
      {
        "id": 41923498,
        "name": "WEB PAGE",
        "weight": "1.00000000"
      },
      {
        "id": 33736409,
        "name": "DBI",
        "weight": "1.00000000"
      },
      {
        "id": 33385994,
        "name": "BSMPERMITS",
        "weight": "1.00000000"
      },
      {
        "id": 39231762,
        "name": "STREETSPACEREQUEST",
        "weight": "1.00000000"
      }
    ],
    [
      {
        "id": 41923373,
        "name": "WEB",
        "weight": "0.50000000"
      }
    ]
  ]
}
```

 If the `--writePrimary` is set true `true` a second text file is created for each strong domain containing the terms in the first block of each strong domain.


## Evaluation Datasets

[![DOI](https://zenodo.org/badge/DOI/10.5281/zenodo.3647613.svg)](https://doi.org/10.5281/zenodo.3647613) [![DOI](https://zenodo.org/badge/DOI/10.5281/zenodo.3647642.svg)](https://doi.org/10.5281/zenodo.3647642) [![DOI](https://zenodo.org/badge/DOI/10.5281/zenodo.3647647.svg)](https://doi.org/10.5281/zenodo.3647647) [![DOI](https://zenodo.org/badge/DOI/10.5281/zenodo.3647651.svg)](https://doi.org/10.5281/zenodo.3647651) [![DOI](https://zenodo.org/badge/DOI/10.5281/zenodo.3647656.svg)](https://doi.org/10.5281/zenodo.3647656)

We evaluated D4 using three collections of data from [New York City](https://opendata.cityofnewyork.us/), and one collection of data from the [State of Utah](https://opendata.utah.gov/). Both datasets were downloaded from the [Socrata API](http://api.us.socrata.com/api/catalog/v1). We are making the data available as five different repositories:

- [Processed Evaluation Data and Ground Truth Domains](https://doi.org/10.5281/zenodo.3647613): This repository contains the equivalence class files for all four dataset that we used to evaluate D4 together with the ground truth domains.
- [NYC Open Datasets - Education](https://doi.org/10.5281/zenodo.3647642): Downloaded tab-delimited data files for tables in the Education dataset (downloaded on 2016-11-22).
- [NYC Open Datasets - Finance](https://doi.org/10.5281/zenodo.3647647): Downloaded tab-delimited data files for tables in the Finance dataset (downloaded on 2016-11-22).
- [NYC Open Datasets - Services](https://doi.org/10.5281/zenodo.3647651): Downloaded tab-delimited data files for tables in the Services dataset (downloaded on 2016-11-22).
- [Utah Open](https://doi.org/10.5281/zenodo.3647656): Downloaded tab-delimited data files for tables in the State of Utah dataset (downloaded on 2019-09-27).
