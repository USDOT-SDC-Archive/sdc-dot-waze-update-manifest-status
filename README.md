# sdc-dot-waze-update-manifest-status
This lambda function is responsible for updating the manifest status in dynamoDB.

<a name="toc"/>

## Table of Contents

[I. Release Notes](#release-notes)

[II. Overview](#overview)

[III. Design Diagram](#design-diagram)

[IV. Getting Started](#getting-started)

[V. Unit Tests](#unit-tests)

[VI. Support](#support)

---

<a name="release-notes"/>


## [I. Release Notes](ReleaseNotes.md)
TO BE UPDATED

<a name="overview"/>

## II. Overview
The primary function that this lambda function serves:
* **update_manifest_status** - updates the manifest status or Filestatus to completed for a particular batch id in the DynamoDB table.

<a name="design-diagram"/>

## III. Design Diagram

![sdc-dot-waze-update-manifest-status](images/waze-data-persistence.png)

<a name="getting-started"/>

## IV. Getting Started

The following instructions describe the procedure to build and deploy the lambda.

### Prerequisites
* NA 

---
### ThirdParty library

*NA

### Licensed softwares

*NA

### Programming tool versions

*Python 3.6


---
### Build and Deploy the Lambda

#### Environment Variables
Below are the environment variables needed :- 

DDB_MANIFEST_TABLE_ARN - {arn_of_manifest_table_in_dynamodb}

DDB_MANIFEST_FILES_INDEX_NAME - {manifest_files_index_name_in_dynamodb}

#### Build Process

**Step 1**: Setup virtual environment on your system by foloowing below link
https://docs.aws.amazon.com/lambda/latest/dg/with-s3-example-deployment-pkg.html#with-s3-example-deployment-pkg-python

**Step 2**: Create a script with below contents e.g(sdc-dot-waze-update-manifest-status.sh)
```#!/bin/sh

cd sdc-dot-waze-update-manifest-status
zipFileName="sdc-dot-waze-update-manifest-status.zip"

zip -r9 $zipFileName common/*
zip -r9 $zipFileName lambdas/*
zip -r9 $zipFileName README.md
zip -r9 $zipFileName update_manifest_status_handler_main.py
zip -r9 $zipFileName root.py
```

**Step 3**: Change the permission of the script file

```
chmod u+x sdc-dot-waze-update-manifest-status.sh
```

**Step 4** Run the script file
./sdc-dot-waze-update-manifest-status.sh

**Step 5**: Upload the sdc-dot-waze-update-manifest-status.zip generated from Step 4 to a lambda function via aws console.

[Back to top](#toc)

---
<a name="unit-tests"/>

## V. Unit Tests

TO BE UPDATED

---
<a name="support"/>

## VI. Support

For any queries you can reach to support@securedatacommons.com
---
[Back to top](#toc)