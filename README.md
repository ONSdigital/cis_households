<img src="https://github.com/datasciencecampus/awesome-campus/blob/master/ons_logo.png">

![Python Version](https://img.shields.io/badge/Python-3.6.8-blue.svg)
[![PySpark version](https://img.shields.io/badge/PySpark-2.4-blue)](https://spark.apache.org/docs/latest/api/python/)
[![black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/ambv/black)
[![pre-commit](https://img.shields.io/badge/pre--commit-enabled-brightgreen?logo=pre-commit&logoColor=white)](https://github.com/pre-commit/pre-commit)
[![status: active](https://github.com/GIScience/badges/raw/master/status/active.svg)](https://github.com/GIScience/badges#active)

# CIS Households

This repository contains the data engineering pipeline for the Office for National Statistics [COVID-19 and Respiratory Infections Survey (CRIS)](https://www.ons.gov.uk/surveys/informationforhouseholdsandindividuals/householdandindividualsurveys/covid19andrespiratoryinfectionssurveycris/aboutthestudy).

This pipeline was developed and used in production for the predecessor survey to CRIS, the [Coronavirus (COVID-19) Infection Survey (CIS)](https://www.ons.gov.uk/peoplepopulationandcommunity/healthandsocialcare/conditionsanddiseases/bulletins/coronaviruscovid19infectionsurveypilot/previousReleases), and is in the process of being updated for CRIS.


**Please note that this project is open for external reuse and review but closed for contribution.**

# Workflow

The overall pipeline takes the raw responses data from survey participants, combines these with the historical data from previous CIS and CRIS participant responses, applies any necessary transformations and generates an output data file for passing to a separate analysis pipeline.

The pipeline is designed to be executed on a Spark cluster within a secure ONS infrastructure (DAP). All input and output data is strictly contained within that secure environment.

The pipeline is structured by a series of defined "stages" which group functional chunks of code together. The sequential ordering of stages is controlled by configuration YAML files - allowing the execution of a particular stage to be adjusted and toggled on and off as desired. These configuration files are stored within the secure environment and are necessary to successfully execute this code.

The following diagram provides a high level overview of these pipeline stages for CRIS.

```mermaid
flowchart TD
    data_1[(Raw\nParticipant\nData)] --> SP_1[Copy Table]
    data_2[(CIS/CRIS\nHistorical\nData)] --> SP_3
    SP_1--> SP_2[Lookup CSVs to Tables]
    SP_2 --> IOP_1(CRIS ETL)
    IOP_1 --> IOP_2(Participant ETL)
    IOP_2 --> SP_3[Union Historical Visits]
    SP_3 --> SP_4[Join Participant ID]
    SP_4 --> TA_1{{Visit Transforms}}
    TA_1 --> TA_2{{Job Transforms}}
    TA_2 --> TA_3{{Demogrpahic Transforms}}
    TA_3 --> TA_4{{Filter Dataframe}}
    TA_4 --> SP_5[Validate Survey Responses]
    SP_5 --> IOP_3(Output Report)
    IOP_3 --> IOP_4(Export Report)
    SP_5 --> IOP_5(Export Data to Analysis)
    subgraph Legend
          direction LR
        start1[ ] --> SP[Supporting Process] --> stop1[ ]
        start2[ ] --> IOP(Input/Output Process) --> stop2[ ]
        start3[ ] --> TA{{Transform Actions}} --> stop3[ ]
        DAT[(Input Data)] --> stop4[ ]
    end
classDef green fill:#2FAD43,stroke:#333,stroke-width:4px,color:#fff;
classDef red fill:#CA3434,stroke:#333,stroke-width:4px,color:#fff;
classDef blue fill:#2F6AAD,stroke:#333,stroke-width:4px,color:#fff;
classDef data fill:#E9E074,stroke:#333,stroke-width:4px;
classDef empty height:0px;
class SP,SP_1,SP_2,SP_3,SP_4,SP_5 green
class IOP,IOP_1,IOP_2,IOP_3,IOP_4,IOP_5 red
class TA,TA_1,TA_2,TA_3,TA_4 blue
class start1,start2,start3,stop1,stop2,stop3,stop4 empty
class data_1,data_2,DAT data
```

## Directories and files of note

The hierarchy of folders (packages and subpackages) generally reflects how specific and abstract the code is. Code that is lower-level in abstraction and is more project-specific will sit deeper in the hierarchy.

Modules in the package are named semantically, so describe the types of operations that their contents perform.

Descriptions of project directories and other significant files:
* `cishouseholds/` - the core package in project. Code that is not project specific sits in the modules directly under this directory (i.e. data engineering functions and spark/hdf utilities that could be used in other projects)
    * `cishouseholds/pipeline` - code that is specific to the project sits within this sub-package
        * `cishouseholds/pipeline/run.py` - the pipeline entry-point, which is executed to run the pipeline. This file assumes the presence of the necessary configuration YAML files.
        * `cishouseholds/pipeline/pipeline_stages.py` - contains the definitions of most pipeline stages (except for input file processing stage; see `cishouseholds/pipeline/input_file_stages.py`), which are the highest level groups of logic in the form of an ETL process. In practice, the execution and ordering of these stages is determined by the configuration YAML files.
        * `cishouseholds/pipeline/input_file_stages.py` - pipeline stages for processing input data files are configured here, using a reusable function factory
        * `cishouseholds/pipeline/pre_union_transformations.py` - functions containing the transformation logic of the pre-union pipeline stages. These exist to allow us to integration test the transformations independents of the extract and load parts of the stages. Usually includes calls to multiple low-level functions
        * `cishouseholds/pipeline/lookup_and_regex_transformations.py` - functions containing the transformation logic of the lookup and regex based pipeline stages. These exist to allow us to integration test the transformations independents of the extract and load parts of the stages. Usually includes calls to multiple low-level functions
        * `cishouseholds/pipeline/post_union_transformations.py` - functions containing the transformation logic of the post-union pipeline stages. These exist to allow us to integration test the transformations independents of the extract and load parts of the stages. Usually includes calls to multiple low-level functions
        * `cishouseholds/pipeline/manifest.py` - a class used to generate a manifest file to trigger automated export of data produced by the pipeline
* `docs/` - package documentation that is built and hosted as HTML by Github Actions based on the `main` branch code. See [documentation for sphinx for updating or building docs manually](https://www.sphinx-doc.org/en/master/)
* `dummy_data_generation/` - dummy data schemata and functions to generate datesets. Used in regression tests for input processing functions
* `tests/` - tests for the code under `cishouseholds/`. Roughly follows the structure of the package, but has become out of date during refactors over time. Searching for references to the current function name is the most reliable way to identify associated tests
    * `tests/conftest.py` - reusable functions and pytest fixtures that can be accessed by any pytest tests
* `CONTRIBUTING.md` - general guidance for contributing to the project
* `Jenkinsfile` - config for Jenkins pipeline that detects new version tags (git tags) and deploys the built package to Artifactory
* `.github/workflows` - configs for Github Actions

# Office for National Statistics

We are the UK’s largest independent producer of official statistics and its recognised national statistical institute. We are responsible for collecting and publishing statistics related to the economy, population and society at national, regional and local levels. We also conduct the census in England and Wales every 10 years.

# Licence

<!-- Unless stated otherwise, the codebase is released under [the MIT Licence][mit]. -->

The code, unless otherwise stated, is released under [the MIT Licence][mit].

The documentation for this work is subject to [© Crown copyright][copyright] and is available under the terms of the [Open Government 3.0][ogl] licence.

[mit]: LICENCE
[copyright]: http://www.nationalarchives.gov.uk/information-management/re-using-public-sector-information/uk-government-licensing-framework/crown-copyright/
[ogl]: http://www.nationalarchives.gov.uk/doc/open-government-licence/version/3/
