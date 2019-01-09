import argparse
import logging
import csv
import re
import tempfile
import json
import os
import shutil
import subprocess
from FormatLog import FormatLogger
logger = FormatLogger('ingest.log','ingest_failures.log','ingest_status.log',truncate = True)
import get_file

log = logging.getLogger(__name__)
required_field_names = ( # for csv, things ending in 1 are multi-valued, everything else is scalar
    'files', #required only for ingest of files on this machine
    'fulltext_url', # required for pulling the files from urls
    'resource_type1',
    'title1',
    'creator1',
    'license1'
)



def run_ingest_process_csv(csv_path,ingest_command,ingest_path,ingest_depositor,worktype, url = None,debug = None,collection = None, tiff = None):
    logging.basicConfig(
        level=logging.DEBUG if debug else logging.INFO
    )
    logging.basicConfig(level=logging.DEBUG)

    field_names, rows = load_csv(csv_path)
    logger.info('Loading {} objects from file: {}'.format(len(rows), csv_path))# pylint: disable=E1120
    validate_field_names(field_names,url)
    singular_field_names, repeating_field_names = analyze_field_names(field_names)
    base_filepath = os.path.dirname(os.path.abspath(csv_path))
    raw_download_dir = tempfile.mkdtemp()
    logger.write('')
    for row in rows:
        upload_id = row['title1'] if 'identifier1' not in row else row['identifier1']
        try:
            logger.status("uploading",upload_id)
            if 'first_file' in row:
                full_file_path = row['first_file']
            if 'files' in row:
                files_dir = row['files']
            if url: #boolean representing if we are using urls to get relevant file(s)
                logger.status("downloading %s"%(row['fulltext_url']))
                files_dir, full_file_path = rip_files_from_url(row,raw_download_dir)
                #full_file_path  = get_file.download_file(row['fulltext_url'],dwnld_dir = raw_download_dir)
                row['files'] = files_dir
                row['first_file'] = full_file_path
            if tiff: # if we want to generate a tiff, and have it be the primary file
                if 'files' not in row:
                    raise ValueError("no files "+str(row))
                if isinstance(row['files'], list):
                    files_dir,full_file_path =  make_tiff_from_file(full_file_path,row['files'],True)
                elif isinstance(row['files'], str) and os.path.isdir(row['files']):
                    files_dir, full_file_path = make_tiff_from_file(full_file_path)
                else:
                    raise ValueError("no files, cause files is not string or path to dir "+str(row))
                row['files'] = files_dir
                row['first_file'] = full_file_path
            metadata = create_repository_metadata(row, singular_field_names, repeating_field_names)#todo
            write_metadata_and_ingest(metadata,row,raw_download_dir,base_filepath,ingest_command,ingest_path,ingest_depositor,worktype, url,debug,collection,tiff)
            # at this point the metadata is a dictionary
            # of all the metadata where reapeating values are key : [value,value]
            # and scalars are key : value
            # the keys are exactly as they will be mapped in hyrax ie "creator" : ["Yoshikami, Katie-Lynn"]
            # instead of "creator1" or any numbered item.
            logger.success("Ingested",upload_id)
        except Exception as e:
            logger.failure("%s was not ingested - %s:%s" % (upload_id,e.__class__.__name__,e) )
            if logger.num_success == 0 and logger.num_fail >= 5:
                print("Warning: Ingest Failed frist 5 in a row!")

        logger.status('End of',upload_id,'\n')# pylint: disable=E1121
    if not debug:
        logger.status('Removing downloaded files from directory tree')# pylint: disable=E1120
        shutil.rmtree(raw_download_dir, ignore_errors=True)

def do_ingest_with_json(json_file,ingest_command,ingest_path,ingest_depositor,worktype,
    url = None,debug = None,collection = None, tiff = None):
    """ a function that ingests works given a json file containing the metadata for the works 
    """
    with open(json_file,'r') as jf:
        rows = json.load(jf)
        ### required for only certain types of ingest ###
        raw_download_dir = tempfile.mkdtemp() # for url downloads
        base_filepath = os.path.dirname(os.path.abspath(json_file)) #this is where files are if we dont need to download them
        ################################################
    logger.info('Loading {} objects from file: {}'.format(len(rows), json_file))# pylint: disable=E1120
    for row in rows:
        try:
            upload_id = row['title'] if 'identifier' not in row else row['identifier']
            logger.status("uploading",upload_id)

            validate_metadata_json(row,url) # ensures that the required stuff is there and that its the right type
            if not url:
                files_dir=row['files']
                full_file_path=row['first_file']
            if url:
                files_dir, full_file_path = rip_files_from_url(row,raw_download_dir)
            if tiff:
                if not os.path.isdir(files_dir):
                    files_dir,full_file_path = make_tiff_from_file(full_file_path,new_dir=True)
                else:
                    files_dir,full_file_path = make_tiff_from_file(full_file_path)


            ### prepare row for ingest ###
            row['files'] = files_dir
            row['first_file'] = full_file_path
            metadata = {}
            for key in row:
                if key != 'files' and key != 'first_file' and key != 'resources' and key != 'fulltext_url':
                    metadata[key] = row[key] 

            ##############################

            write_metadata_and_ingest(metadata,row,raw_download_dir,base_filepath,ingest_command,ingest_path,ingest_depositor,worktype, url,debug,collection,tiff)
            logger.success("Ingested",upload_id)
        except Exception as e:
            logger.failure("%s was not ingested - %s:%s" % (upload_id,e.__class__.__name__,e) )
            if logger.num_success == 0 and logger.num_fail >= 5:
                print("Warning: Ingest Failed frist 5 in a row!")
        logger.status('End of',upload_id,'\n')# pylint: disable=E1121

    if not debug:
        logger.status('Removing downloaded files from directory tree')# pylint: disable=E1120
        shutil.rmtree(raw_download_dir, ignore_errors=True)

def validate_metadata_json(metadata,use_url):
    """ 
    Desc: ensures that the metadata is in the right form by getting list of required scalars and list of required 
    multi-valued metadata objects and ensure that scalars are not lists and that multivalues are.
    Args: metadata (dict): all metadat including the files, first_file, fulltext_url type stuff
    Returns: its a void function
    """
    log.debug('Validating field names for json ingest')
    scalars, lists = analyze_field_names(required_field_names)
    try:
        for value in scalars:
            assert value in metadata
            assert not isinstance(metadata[value], list)
        for value in lists:
            assert value in metadata
            assert isinstance(metadata[value], list)
        if use_url:
            assert 'fulltext_url' in metadata
        if not use_url:
            assert 'files' in metadata
    except Exception as e:
        logger.critical("%s is a required fields and was not found %s" % (value,e) )# pylint: disable=E1120
        raise
    return


def rip_files_from_url(row,raw_download_dir):
    """ 
    Desc: takes in a row of metadata including 'fulltext_url' and optionally 'resources'
        downloads all files to new directory insdie the raw_download_dir directory returns 
        path to the dir containing the files, and the first files path
    Args: row (dict): metadata for the work
          raw_download_dir (str): path to the place these files should be stored
    returns: tuple: first element is the path to the directory containing relevant resources:
                    second element is the path to the primary file for the work
    """
    if 'identifier' in row and row['identifier']:
        if isinstance(row['identifier'],list):
            ID = row['identifier'][0]
        else:
            ID = row['identifier']

        proj_dir = os.path.join(raw_download_dir,ID)
        get_file.mkdir(proj_dir)

        if not os.path.exists(proj_dir):
            logger.error('could not create project dir')
            raise FileNotFoundError('could not create project dir')
    else:
        proj_dir = tempfile.mkdtemp(dir=raw_download_dir)


    if 'resources' in row and row['resources']:
        for resource in row['resources']:
            get_file.download_file(resource,dwnld_dir = proj_dir)
    full_file_path  = get_file.download_file(row['fulltext_url'],dwnld_dir = proj_dir)
    return proj_dir, full_file_path

def make_tiff_from_file(full_file_path,files = None,new_dir = False):
    """ generates a tiff for the file at full_file_path, places it in the same directory.
        if new_dir flag evaluates as true, then will create a new directory and place both files there 
    """
    if files is None:
        files = []
    generated_tiff = get_file.create_tiff_imagemagick(full_file_path)
    tiff_name = os.path.basename(generated_tiff)
    if new_dir: #prob not actually gonna use this, im confused.
        new_dir = get_file.create_dir_for([full_file_path,generated_tiff]+files)
        return new_dir, os.path.join(new_dir,tiff_name)
    return os.path.dirname(generated_tiff),generated_tiff

def write_metadata_and_ingest(metadata,row,raw_download_dir,base_filepath,ingest_command,ingest_path,ingest_depositor,worktype, url = None,debug = None,collection = None, tiff = None):
    """ takes the metadata for a work,  ingests the work into hyrax using rake task in config.py
    """
    metadata_temp_path = tempfile.mkdtemp()
    metadata_filepath = os.path.join(metadata_temp_path, 'metadata.json')

    try:
        with open(metadata_filepath, 'w') as repo_metadata_file:
            json.dump(metadata, repo_metadata_file, indent=4)
            log.debug('Writing to {}: {}'.format(metadata_filepath, json.dumps(metadata)))
        try:
            first_file, other_files = find_files(row['files'], row.get('first_file'), base_filepath)
            # TODO: Handle passing existing repo id
            repo_id = repo_import(metadata_filepath, metadata['title'], first_file, other_files, None,
                                  ingest_command,
                                  ingest_path,
                                  ingest_depositor,
                                  worktype,
                                  collection)
            # TODO: Write repo id to output CSV
        except Exception as e:
            # TODO: Record exception to output CSV
            raise e
    finally:
        if (not debug) and os.path.exists(metadata_filepath):
            shutil.rmtree(metadata_temp_path, ignore_errors=True)
            #shutil.rmtree(raw_download_dir, ignore_errors=True)


def load_csv(filepath):
    """
    Reads CSV and returns field names, rows
    """
    log.debug('Loading csv')
    with open(filepath) as csvfile:
        reader = csv.DictReader(csvfile)
        return reader.fieldnames, list(reader)


def validate_field_names(field_names,use_url):
    """
    ensures the required fields are present in the data source
    """
    log.debug('Validating field names')
    for field_name in required_field_names:
        if field_name == 'files':
            if use_url:
                continue #we dont need this if we use urls instead
        if field_name == 'fulltext_url':
            if not use_url:
                continue #we dont need this if we have paths instead of urls
        try:
            assert field_name in field_names
        except Exception as e:
            logger.critical('field %s not in fieldnames' % (field_name) )# pylint: disable=E1120
            raise e

def analyze_field_names(field_names):
    """
    Desc: a function that decides what fields are has_many and what are single_value
        aka what will be a list of values versus single value
    Args:
        field_names (list): all the field names from the original metadata information provided
    Returns: touple of where first value is the names of items which will be single values.
        second item of touple is the names of fields which will be lists and are \
        currently labeled like creator1 creator2 creator3

    """
    repeating_field_names = set()
    singular_field_names = set()
    for field_name in sorted(field_names):
        match = re.fullmatch(r'(.+)(\d+$)', field_name)
        if not match:
            singular_field_names.add(field_name)
        else:
            name_part, number_part = match.groups()
            while re.match(r'\d',name_part[-1]):
                number_part = name_part[-1] + number_part
                name_part = name_part[:-1]
            if number_part == '1':
                repeating_field_names.add(name_part)
            elif name_part not in repeating_field_names:
                singular_field_names.add(field_name)
    if 'files' in singular_field_names:
        singular_field_names.remove('files')
    if 'fulltext_url' in singular_field_names:
        singular_field_names.remove('fulltext_url')
    if 'first_file' in singular_field_names:
        singular_field_names.remove('first_file')
    logger.status('Singular field names: {}'.format(singular_field_names))# pylint: disable=E1120
    logger.status('Repeating field names: {}'.format(repeating_field_names))# pylint: disable=E1120
    return singular_field_names, repeating_field_names


def create_repository_metadata(row, singular_field_names, repeating_field_names):
    """
    DESC: given a line from the csv this function returns a dictionary of metadata
         with lists instead of repeated fileds followed by a number
         ie { "title": "joe","creator1": "larry", "creator2" : "james" }
         becomes { "title": "joe","creator": ["larry", "james"] }
    Args:
        row (dict): a line from the csv with fieldname:value (as a dict)
        singular_field_names (set): a list of fields that are not to be listsself.
            calculated in analyze_field_names()
        repeating_field_names (set): a list of field names which will be lists (has many) not single value
    Return: dict representing metadata
    """
    metadata = dict()
    for field_name in singular_field_names:
        metadata[field_name] = row[field_name] if row[field_name] != '' else None
    for field_name in repeating_field_names:
        metadata[field_name] = list()
        field_incr = 1
        while True:
            field_name_incr = '{}{}'.format(field_name, field_incr)
            if field_name_incr in row:
                if row[field_name_incr] != '':
                    metadata[field_name].append(row[field_name_incr])
            else:
                break
            field_incr += 1

    return metadata


def find_files(row_filepath, row_first_filepath, base_filepath):
    """
    Desc: this function will locate all the files and check to ensure the primary file is present
    Args: row_filepath (str) the path to the file or directory that contains relevent resources.
        row_first_file is the main resource to be used
        base_filepath: is just the dir containing the csv, used for non url ingests

    Return: touple
        first element (str): path to the primary file
        second element (set): list of other files relating to the work (does not include primary file)
    """
    filepath = os.path.join(base_filepath, row_filepath)
    #so os.path.join will just return the second path, if the paths given are entirely disimilar it seems
    #so /home/me/dir and /tmp/files/file -> /tmp/files/file
    if not os.path.exists(filepath):
        raise FileNotFoundError(filepath)
    files = set()
    if os.path.isfile(filepath):
        files.add(filepath)
    else:
        for path, _, filenames in os.walk(filepath):
            for filename in filenames:
                files.add(os.path.join(path, filename))
    # Make sure at least one file
    if not files:
        raise FileNotFoundError('Files in {}'.format(filepath))
    
    # Either a row_first_filepath or only one file
    if not (row_first_filepath or len(files) == 1):
        raise FileNotFoundError('First file')
    if row_first_filepath:
        first_file = os.path.join(base_filepath, row_first_filepath)
        if not os.path.exists(first_file):
            raise FileNotFoundError(first_file)
        if not first_file in files:
            raise FileNotFoundError('{} not in files'.format(first_file))
    else:
        first_file = list(files)[0]
    files.remove(first_file)
    return first_file, files


def repo_import(repo_metadata_filepath, title, first_file, other_files, repository_id, ingest_command,
                ingest_path, ingest_depositor,worktype,collection = None):
    """
    Desc: this function takes in relevant information and paths and calls the rake
        task to ingest the work into Hyrax
    Args:
        repo_metadata_filepath (str): path to the file which contains nested json
            representing the metadata (basically the python dict in a file)
        title (str): the title of the work to be uploaded
        first_file (str): the path to the primary file
        other_files (set): list of the rest of the file paths
        repository_id (str or None): [Optional] id of the original work to which
            this is an update if None this is a new work to add
        ingest_command (str): the command to execute the rake task - set in the
            config.py file
        ingest_path (str): the directory of our rails project - set in config.py
        ingest_depositor (str): the username of the person depositing the
            information - set in the config.py file
        worktype(str): the work type in hyrax ie Etd
        collectoin (str): the id of the collection in hyrax to add this work to
    Returns: the id of the work in hyrax
    """
    logger.info('Importing %s.', title)
    # rake gwss:ingest_etd -- --manifest='path-to-manifest-json-file' --primaryfile='path-to-primary-attachment-file/myfile.pdf' --otherfiles='path-to-all-other-attachments-folder'
    command = ingest_command.split(' ') + ['--',
                                           '--manifest=%s' % repo_metadata_filepath,
                                           '--primaryfile=%s' % first_file,
                                           '--depositor=%s' % ingest_depositor,
                                           '--worktype=%s' % worktype]
    if collection:
        command += ['--collection=%s' % collection]
    if other_files:
        command.extend(['--otherfiles=%s' % '{|,|}'.join(other_files)]) # our files have commas
    if repository_id:
        log.info('%s is an update.', title)
        command.extend(['--update-item-id=%s' % repository_id])
    space = "\r" + ''.join([' ']*200)
    logger.info(space+"\r\tCommand is: %s\n" % ' '.join(command))# pylint: disable=E1120
    output = subprocess.check_output(command, cwd=ingest_path)
    repository_id = output.decode('utf-8').rstrip('\n')
    logger.info('Repository id for',title,'is', repository_id)# pylint: disable=E1120
    return repository_id


if __name__ == '__main__':
    import config

    parser = argparse.ArgumentParser(description='Loads into digitalWPI from CSV (or Json)')
    parser.add_argument('--debug', action='store_true')
    parser.add_argument('file', help='filepath of CSV file or Json')
    parser.add_argument('--url', action='store_true',help='if this flag is set, it will look for fulltext_url instead of files')
    parser.add_argument('--worktype',type=str,help='The Hyrax work type of the works [default: Etd]',default="Etd")
    parser.add_argument('--collection',type=str,help='the id of the collection to add this work to in hyrax',default=None)
    parser.add_argument('--tiff',action='store_true',help='if flag is used will generate a tiff from primary file and use that as primary file')
    parser.add_argument('--json', action='store_true',help='if the file containing the metadata for the works is a json file, use this flag.')
    parser.add_argument('--print',type=int,help="how much of the log messages should be printed"+\
        "\n1: status,errors,warnings,successful ingests,failed ingests, critical failurs, ending summary\n"+\
        "2: everything but status\n3: just success and failues + summary\n4+: nothing but critical failues",default=1)
    args = parser.parse_args()
    logger.set_print_level(args.print)
    logger.status('Start of ingest {}'.format(args))# pylint: disable=E1120
    if args.json:
        do_ingest_with_json(args.file,config.ingest_command, config.ingest_path, config.ingest_depositor,
            args.worktype,url = args.url,debug = args.debug,collection = args.collection,tiff = args.tiff)
    else:
        run_ingest_process_csv(args.file,config.ingest_command, config.ingest_path, config.ingest_depositor,
            args.worktype,url = args.url,debug = args.debug,collection = args.collection,tiff = args.tiff)
logger.close()