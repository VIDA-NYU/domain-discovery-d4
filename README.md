Data-Driven Domain Discovery (D4)
=====================

[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0) [![DOI](https://zenodo.org/badge/238518915.svg)](https://zenodo.org/badge/latestdoi/238518915)


## About

D4 implements a data-driven domain discovery approach for collections of related tabular (structured) datasets. Given collection of datasets, D4 outputs  a set of domains discovered from the collection in a holistic fashion, by taking all the data into account.

Similar to word embedding methods such as Word2Vec, D4 gathers contextual information for terms. But unlike these methods which aim to build context for terms in unstructured text, we aim to capture context for terms within columns in a set of tables. The intuition is that terms from the same domain frequently occur together in columns or at least with similar sets of terms. 


## Installation

The D4 jar-file can be build using [Apache Maven](https://maven.apache.org/). After cloning the repository, run `mvn clean install` to build the D4 jar-file.

```
git clone git@github.com:VIDA-NYU/domain-discovery-d4.git
cd domain-discovery-d4
mvn clean install
cp target/D4-jar-with-dependencies.jar /home/user/lib/D4.jar
```

## Domain Discovery Pipeline

The D4 algorithm operates on a set of CSV or tab-delimited TSV files. All input files are expected within a single directory. D4 will consider all files in that directory with suffix `.csv`, `.csv.gz`, `.tsv`, or `.tsv.gz`. The first line in each file is expected to contain the column names. The `D4.jar` file supports nine different commands:

```
$> java -jar /home/user/lib/D4.jar --help

Usage:
  <command> [

      Data preparation
      ----------------
      columns
      text-columns
      term-index
      eqs

      D4 pipeline
      -----------
      signatures
      expand-columns
      local-domains
      strong-domains

      Explore Results
      ---------------
      export

  ] <args>
```

The first four commands are required for transforming the input datasets into the internal format that is used by D4.

### Data Preparation

**Generate Column Files:** The first command converts a set of CSV files into a set of column files, one file for each column in the dataset collection. Column files contain a list of distinct values for the respective column. Each column has a unique identifier. Column metadata (i.e., column name and dataset file) are written to a metadata file. During this step the user has the option to convert all column value to upper case (to make the domain-discovery process case-insensitive).

```
$> java -jar  /home/user/lib/D4.jar columns --help
D4 - Data-Driven Domain Discovery - Version (0.27.0)

columns
  --input=<directory> [default: 'tsv']
  --metadata=<file> [default: 'columns.tsv']
  --toupper=<boolean> [default: true]
  --output=<directory> [default: 'columns']
```

**Identify Text Columns:** Domain discovery often focuses of textual values only. The text column identification step allows to identify those columns in the data collections that have (large) fraction of text values. The `threshold` parameter allows to control the fraction of text values a column has to have in order to be considered as a text column. By default, any columns with more than half of the distinct values being classified as text is considered a text column (GT0.5 = greater than 0.5).

```
$> java -jar  /home/user/lib/D4.jar text-columns --help
D4 - Data-Driven Domain Discovery - Version (0.27.0)

text-columns
  --input=<directory> [default: 'columns']
  --threshold=<constraint> [default: 'GT0.5']
  --output=<file> [default: 'text-columns.txt']
```

**Create Index of Unique Terms in the Data Collection:** The term index contains a list of unique terms across all columns in the collection. In this step the user has the option to consider only a subset of all columns in the collection (e.g., only those columns that were classified as text columns). If you want to consider all columns in the data collection you can skip the previous step and let the command line parameter `input` point the the directory that contains the column files that were generated in the first step.

```
$> java -jar /home/user/lib/D4.jar term-index --help
D4 - Data-Driven Domain Discovery - Version (0.27.0)

term-index
  --input=<directory | file> [default: 'text-columns.txt']
  --membuffer=<int> [default: 10000000]
  --output=<file> [default: 'text-columns.txt']
```

**Generate Equivalence Classes:** D4 operates on sets of equivalence classes. Equivalence classes are sets of terms that always occur in the same set of columns.

```
$> java -jar /home/user/lib/D4.jar eqs --help
D4 - Data-Driven Domain Discovery - Version (0.27.0)

eqs
  --input=<file> [default: 'term-index.txt.gz']
  --output=<file> [default: 'compressed-term-index.txt.gz']
```

### Domain Discovery

D4 has three main steps: signature generation, column expansion, and domain discovery.

**Robust Signatures:** The first step creates signatures that capture the context of terms taking into account term co-occurrence information over all columns in the collection. These signatures are made robust to noise, heterogeneity, and ambiguity. Robust signatures capture the context of related terms while blending out noise. They are essential to our approach in addressing the challenges of incomplete columns (through column expansion) and term ambiguity in heterogeneous and noisy data.

```
$> java -jar /home/user/lib/D4.jar signatures --help
D4 - Data-Driven Domain Discovery - Version (0.27.0)

signatures
  --eqs=<file> [default: 'compressed-term-index.txt.gz']
  --threads=<int> [default: 6]
  --signatures=<file> [default: 'signatures.txt.gz']
```

**Expanded Columns:** The *column expansion* step addresses the challenge of incomplete columns by adding terms to a column that are likely to belong to the same domain as the majority of terms in the column. D4 makes use of robust signatures to expand columns: it adds a term only if it has sufficient support (controlled by `expandThreshold`) from the robust signatures of terms in the column. This leads to high accuracy in domain discovery. We take an iterative approach to column expansion. The idea is that adding a term to a column may provide additional support for other terms to be added as well (controlled by `iterations` and `decrease`).

```
$> java -jar /home/user/lib/D4.jar expand-columns --help
D4 - Data-Driven Domain Discovery - Version (0.27.0)

expand-columns
  --eqs=<file> [default: 'compressed-term-index.txt.gz']
  --signatures=<file> [default: 'signatures.txt.gz']
  --trimmer=<string> [default: CENTRIST] (alternatives: CONSERVATIVE, LIBERAL)
  --expandThreshold=<constraint> [default: 'GT0.25']
  --decrease=<double> [default: 0.05]
  --iterations=<int> [default: 5]
  --threads=<int> [default: 6]
  --columns=<file> [default: 'expanded-columns.txt.gz']
```

**Local Domains:** This step derives from each column a set of domain candidates, called *local domains*. Local domains are clusters of terms in an (expanded) column that are likely to belong to the same type.

```
$> java -jar /home/user/lib/D4.jar local-domains --help
D4 - Data-Driven Domain Discovery - Version (0.27.0)

local-domains
  --eqs=<file> [default: 'compressed-term-index.txt.gz']
  --columns=<file> [default: 'expanded-columns.txt.gz']
  --signatures=<file> [default: 'signatures.txt.gz']
  --trimmer=<string> [default: CENTRIST]
  --threads=<int> [default: 6]
  --localdomains=<file> [default: 'local-domains.txt.gz']
```

**Strong Domains:** In this step, D4 applies a data-driven approach to narrow down the set of local domains and create a smaller set of *strong domains* to be presented to the user.

```
$> java -jar /home/user/lib/D4.jar strong-domains --help
D4 - Data-Driven Domain Discovery - Version (0.27.0)

strong-domains
  --eqs=<file> [default: 'compressed-term-index.txt.gz']
  --localdomains=<file> [default: 'local-domains.txt.gz']
  --domainOverlap=<constraint> [default: 'GT0.5']
  --supportFraction=<double> [default: 0.25]
  --threads=<int> [default: 6]
  --strongdomains=<file> [default: 'strong-domains.txt.gz']
```


### Explore Results

**Export Domains:** The discovered domains can be exported as JSON files for exploration. This step can be applied to either the local domains or strong domains (or any subset or combination of the domains in both steps). For each domain a separate file will be created in the output directory, containing the list of terms as well as the list of columns that the domain was assigned to.

```
$> java -jar /home/user/lib/D4.jar export --help
D4 - Data-Driven Domain Discovery - Version (0.27.0)

export
  --eqs=<file> [default: 'compressed-term-index.txt.gz']
  --terms=<file> [default: 'term-index.txt.gz']
  --columns=<file> [default: 'columns.tsv']
  --domains=<file> [default: 'strong-domains.txt.gz']
  --output=<direcory> [default: 'domains']
```

## Evaluation Datasets

[![DOI](https://zenodo.org/badge/DOI/10.5281/zenodo.3647613.svg)](https://doi.org/10.5281/zenodo.3647613) [![DOI](https://zenodo.org/badge/DOI/10.5281/zenodo.3647642.svg)](https://doi.org/10.5281/zenodo.3647642) [![DOI](https://zenodo.org/badge/DOI/10.5281/zenodo.3647647.svg)](https://doi.org/10.5281/zenodo.3647647) [![DOI](https://zenodo.org/badge/DOI/10.5281/zenodo.3647651.svg)](https://doi.org/10.5281/zenodo.3647651) [![DOI](https://zenodo.org/badge/DOI/10.5281/zenodo.3647656.svg)](https://doi.org/10.5281/zenodo.3647656)

We evaluated D4 using three collections of data from [New York City](https://opendata.cityofnewyork.us/), and one collection of data from the [State of Utah](https://opendata.utah.gov/). Both datasets were downloaded from the [Socrata API](http://api.us.socrata.com/api/catalog/v1). We are making the data available as five different repositories:

- [Processed Evaluation Data and Ground Truth Domains](https://doi.org/10.5281/zenodo.3647613): This repository contains the equivalence class files for all four dataset that we used to evaluate D4 together with the ground truth domains.
- [NYC Open Datasets - Education](https://doi.org/10.5281/zenodo.3647642): Downloaded tab-delimited data files for tables in the Education dataset (downloaded on 2016-11-22).
- [NYC Open Datasets - Finance](https://doi.org/10.5281/zenodo.3647647): Downloaded tab-delimited data files for tables in the Finance dataset (downloaded on 2016-11-22).
- [NYC Open Datasets - Services](https://doi.org/10.5281/zenodo.3647651): Downloaded tab-delimited data files for tables in the Services dataset (downloaded on 2016-11-22).
- [Utah Open](https://doi.org/10.5281/zenodo.3647656): Downloaded tab-delimited data files for tables in the State of Utah dataset (downloaded on 2019-09-27).
