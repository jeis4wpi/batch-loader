# batch-loader
Application for batch loading GW ScholarSpace

## Setup
Requires Python >= 3.5

1. Get this code.

        `git clone git@github.com:DigitalWPI/batch-loader.git`
        OR
        `git clone https://github.com/DigitalWPI/batch-loader.git`

2. Create a virtualenv.

        virtualenv -p python3 ENV
        source ENV/bin/activate

3. install needed python libraries 

        pip install -r requirements.txt

4. Copy configuration file.

        cp example.config.py config.py

5. Edit configuration file. The file is annotated with descriptions of the configuration options.

## Running
To run batch-loader:

    `python batch_loader.py <path to csv>`
    OR if instead of haveing the column `files` you have the column `fulltext_url` of the related resource
    `python batch_loader.py <path to csv> --url`
    see example.csv and url_example.csv
    finally it can also be run on json files, using the same elements as the csv. 
    `python batch_loader.py <path to json file> --json`
    there are more options as well such as what collections to ingest to if your
    rake task can handle that, whether or not to generate tiffs, and print level.
    use `python batch_loader.py --help` to see all the options

## Specification of CSV
1. The first row must contain the field names. (unless --url is given)
2. Fields that take multiple values should be placed in multiple columns.
   Each field name should be appended with an incrementing integer. For
   example, "author1", "author2", "author3". Even if there is only a
   single entry, but the field is repeating, the field name should end with "1".
   (Fields with multiple values will be passed as lists to GWSS.)
3. The following fields are required: files (or fulltext_url for this --url variant), object_type, title, author1,
   type_of_work1, rights.
4. The following fields are optional, but if provided must use these field names:
   first_file, gwss_id. (TODO)
5. Additional fields included in the CSV will be passed to GWSS using the provided
   field names. For example, a "subtitle" field included in the CSV will be
   passed as "subtitle" to GWSS.
6. The ordering of fields is not significant.

## TODO:
1. Support updating when already has a repo id.
2. Write output CSV containing repo id.
3. Error handling when import fails.
