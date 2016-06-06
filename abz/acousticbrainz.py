# Copyright 2014 Music Technology Group - Universitat Pompeu Fabra
# acousticbrainz-client is available under the terms of the GNU
# General Public License, version 3 or higher. See COPYING for more details.

from __future__ import print_function

import json
import os
import sqlite3
import subprocess
import sys
import tempfile
import uuid

try:
    import requests
except ImportError:
    from .vendor import requests

from abz import compat, config

config.load_settings()
conn = sqlite3.connect(config.get_sqlite_file())
VERBOSE = False

RESET = "\x1b[0m"
RED = "\x1b[31m"
GREEN = "\x1b[32m"


def _update_progress(msg, status="...", colour=RESET):
    if VERBOSE:
        sys.stdout.write("%s[%-10s]%s " % (colour, status, RESET))
        print(msg.encode("ascii", "ignore"))
    else:
        sys.stdout.write("%s[%-10s]%s " % (colour, status, RESET))
        sys.stdout.write(msg+"\x1b[K\r")
        sys.stdout.flush()


def _start_progress(msg, status="...", colour=RESET):
    print()
    _update_progress(msg, status, colour)


def add_to_filelist(filepath, reason=None):
    query = """insert into filelog(filename, reason) values(?, ?)"""
    c = conn.cursor()
    c.execute(query, (compat.decode(filepath), reason))
    conn.commit()


def is_valid_uuid(u):
    try:
        u = uuid.UUID(u)
        return True
    except ValueError:
        return False


def is_processed(filepath):
    query = """select * from filelog where filename = ?"""
    c = conn.cursor()
    r = c.execute(query, (compat.decode(filepath), ))
    if len(r.fetchall()):
        return True
    else:
        return False


def run_extractor(input_path, output_path):
    extractor = config.settings["essentia_path"]
    profile = config.settings["profile_file"]
    args = [extractor, input_path, output_path, profile]

    p = subprocess.Popen(args, stderr=subprocess.STDOUT, stdout=subprocess.PIPE)
    (out, err) = p.communicate()
    retcode = p.returncode
    return retcode, out


def submit_features(recordingid, features):
    featstr = json.dumps(features)

    host = config.settings["host"]
    url = compat.urlunparse(('http', host, '/%s/low-level' % recordingid, '', '', ''))
    r = requests.post(url, data=featstr)
    r.raise_for_status()


def process_file(filepath):
    _start_progress(filepath)
    if is_processed(filepath):
        _update_progress(filepath, ":) done", GREEN)
        return

    fd, tmpname = tempfile.mkstemp(suffix='.json')
    os.close(fd)
    os.unlink(tmpname)
    retcode, out = run_extractor(filepath, tmpname)
    if retcode == 2:
        _update_progress(filepath, ":( nombid", RED)
        print()
        print(out)
        add_to_filelist(filepath, "nombid")
    elif retcode == 1:
        _update_progress(filepath, ":( extract", RED)
        print()
        print(out)
        add_to_filelist(filepath, "extractor")
    elif retcode > 0 or retcode < 0:  # Unknown error, not 0, 1, 2
        _update_progress(filepath, ":( unk %s" % retcode, RED)
        print()
        print(out)
    else:
        if os.path.isfile(tmpname):
            try:
                features = json.load(open(tmpname))
                # Recording MBIDs are tagged with _trackid for historic reasons
                recordingids = features["metadata"]["tags"]["musicbrainz_trackid"]
                if not isinstance(recordingids, list):
                    recordingids = [recordingids]
                recs = [r for r in recordingids if is_valid_uuid(r)]
                if recs:
                    recid = recs[0]
                    try:
                        submit_features(recid, features)
                    except requests.exceptions.HTTPError as e:
                        _update_progress(filepath, ":( submit", RED)
                        print()
                        print(e.response.text)
                    add_to_filelist(filepath)
                    _update_progress(filepath, ":)", GREEN)
                else:
                    _update_progress(filepath, ":( badmbid", RED)

            except ValueError:
                _update_progress(filepath, ":( json", RED)
                add_to_filelist(filepath, "json")

    if os.path.isfile(tmpname):
        os.unlink(tmpname)


def process_directory(directory_path):
    _start_progress("processing %s" % directory_path)

    for dirpath, dirnames, filenames in os.walk(directory_path):
        for f in filenames:
            if f.lower().endswith(config.settings["extensions"]):
                process_file(os.path.abspath(os.path.join(dirpath, f)))


def process(path):
    if not os.path.exists(path):
        sys.exit(path + " does not exist")
    path = os.path.abspath(path)
    if os.path.isfile(path):
        process_file(path)
    elif os.path.isdir(path):
        process_directory(path)


def cleanup():
    if os.path.isfile(config.settings["profile_file"]):
        os.unlink(config.settings["profile_file"])
        
def generate_dataset(path):
    """Process a direcotry containing a datase
    
    Args:
        path: the relative or absolute path of the dataser rootdir
    """
    if not os.path.exists(path):
        sys.exit(path + " does not exists!")
    if not os.path.isdir(path):
        sys.exit(path + " is not a directory!")
    datasetdict = scan_dir(os.path.abspath(path))
    print(datasetdict)
    # submit dataset using the Dataset API
    submit_dataset(datasetdict)
    
def scan_dir(path):
    """Scan a directory to create the dataset dictionary basic structure
    
    Args:
        path: the absolute path of the dataser rootdir
    Returns:
        datasetdict: a dictionary with the basic structure of the dataset
    """

    dataset_name = os.path.basename(os.path.normpath(path))
    # handles the basic dataset dictionary structure
    datasetdict = _datasetdict_structure(dataset_name)
    itemuuid = None
    
    for dirName, sudDirNames, fileNames in os.walk(path):
        
        for subdir in sudDirNames:
            tmpdict = {}
            tmpdict['name'] = subdir
            tmpdict['description'] = ''
            tmpdict['recordings'] = []

            # scan each subfolder in the dataset root
            subdir = os.path.join(path, subdir)
    
            for dirName, subDirNames, fileNames in os.walk(subdir):
                ### run the extractor
                for fileName in fileNames:
                    # run the extractor
                    filepath = os.path.join(subdir, fileName)
                    extractor_output = _dataset_item_extractor(filepath)
                    # submit dataset item (md5 check)
                    itemuuid = submit_dataset_item(extractor_output)
                    if not itemuuid==None:
                        itemuuid = itemuuid['itemuuid']
                        tmpdict['recordings'].append(str(itemuuid))
                        _update_progress(filepath, ":) Get UUID", GREEN)
                        print()
                    else:
                        _update_progress(filepath, ":( No UUID", RED)
                        print()
                    
                    os.unlink(extractor_output)
                    
                if len(tmpdict['recordings'])>0:
                    datasetdict['classes'].append(tmpdict)
    
    #print(datasetdict)
    return datasetdict

def _dataset_item_extractor(filepath):
    """Extract lowlevel data from a dataset item
    
    Args:
        filepath:   the fill path of the dataset item to process
    Returns:
        tmpname:    the full path of the generated lowlevel-data json file
    """
    fd, tmpname = tempfile.mkstemp(suffix='.json')
    os.close(fd)
    os.unlink(tmpname)
    # avoid using default config file for datasets items MBID is not required
    config.settings["profile_file"] = ""
    try:
        retcode, out = run_extractor(filepath, tmpname)
        if retcode == 1:
            print()
            print(out)            
            _update_progress(filepath, ":( extract", RED)
            add_to_filelist(filepath, "extractor")
        elif retcode < 0 or retcode > 0:
            print()
            print(out)
            _update_progress(filepath, ":( unknown error", RED)
        else:
            if os.path.isfile(tmpname):
                return tmpname
            else:
                return None
    except KeyboardInterrupt:
        print()
        print("Action interrupted by the user!")
        print("Stopped while extracting data from file:", filepath)
        sys.exit(1)

def _datasetdict_structure(dataset_name):
    """Generates the skeleton for the dataset dictionary
    
    Args:
        dataset_name:   the name of the dataset (the root folder of the dataset)
    Returns:
        datasetdict:    a dictionar ywith the dataset structure
    """
    datasetdict = {}
    datasetdict['name'] = dataset_name
    datasetdict['description'] = ''
    datasetdict['public'] = 'true'
    datasetdict['classes'] = []
    return datasetdict

def submit_dataset_item(filepath):
    """Submit the lowlevel data of a dataset item and get the UUID from the server
    
    ARGS:
        filepath: the path of the temp json file generated by the essentia extractor
    Returns:
        itemuuid: the item UUID retrieved/generated by the server
    """
    with open(filepath) as jsonfile:
        try:
            jsondata = json.load(jsonfile)
        except (TypeError, ValueError):
            print()
            print("Unknown Extractor Error")
            _update_progress(filepath, ":( json format", RED)
            return None
    
    host = config.settings["host"]
    # This is a fake endpoints to be defined
    url = compat.urlunparse(('http', host, '/datasetitem/', '', '',''))
    try:
        r = requests.post(url, data=jsondata)
        if r.status_code==200:
            itemuuid = json.loads(r.json)
            return itemuuid
        else:
            print()
            print(r.status_code)
            _update_progress(filepath, ":( NoUUID", RED)
            # comment this to test with fake uuids
            return None
    except requests.exceptions.HTTPError as e:
        print()
        print(e.response.text)
        _update_progress(filepath, ":( HTTP Error", RED)
        return None
    # send fake uuid
    #test_response = { 'itemuuid': '23456' }
    #return test_response

def submit_dataset(datasetdict):
    """Send the dataset dictionary to the server through the Dataset API
    
    Args:
        datasetdict:    A dataset dictionary as defined in the API doc
    """
    datasettxt = json.dumps(datasetdict)

    host = config.settings["host"]
    url = compat.urlunparse(('http', host, '/datasets/', '', '', ''))
    try:
        r = requests.post(url, data=datasettxt)
        print(r.status_code)
    except requests.exceptions.HTTPError as e:
        print()
        print(e.response.text)
        _update_progress('dataset submission problem', ':( submission', RED)
    