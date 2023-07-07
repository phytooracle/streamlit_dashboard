#!/usr/bin/env python3
"""
Author : Emmanuel Gonzalez
Date   : 2023-06-27
Purpose: Individual plant point cloud fetcher
"""

import argparse
import os
import sys
import re
from pathlib import Path
import json
import requests
import multiprocessing as mp
from datetime import datetime, timedelta
import re
import subprocess as sp
import glob

# --------------------------------------------------
def get_args():
    """Get command-line arguments"""

    parser = argparse.ArgumentParser(
        description='Individual plant point cloud fetcher',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter)

    parser.add_argument('-o',
                        '--out_dir',
                        help='Output directory',
                        required=False,
                        default='plant_point_clouds')

    parser.add_argument('-s',
                        '--season',
                        help='Season during which data were collected',
                        type=str,
                        choices=['10', '11', '12', '13', '14', '15', '16'],
                        required=True)
    
    parser.add_argument('-c',
                        '--crop',
                        help='Crop name of data to download',
                        type=str,
                        choices=['sorghum', 'lettuce', 'NA'],
                        default="NA")
    
    parser.add_argument('-i',
                        '--instrument',
                        help='Instrument (sensor) used to collect phenotype data.',
                        type=str,
                        choices=['FLIR', 'PS2', '3D', 'RGB'],
                        required=True)
    
    parser.add_argument('-l',
                        '--level',
                        help='Level of the data.',
                        type=str,
                        choices=['0', '1', '2', '3', '4'],
                        required=True)
    
    parser.add_argument('-f',
                        '--final_date',
                        help='Final scan date to search for within index file names',
                        nargs='+',
                        required=True)
    
    parser.add_argument('-g',
                        '--genotype',
                        help='Name of genotype/s to collect.',
                        nargs='+')
    
    return parser.parse_args()


#-------------------------------------------------------------------------------
def json_index(date, index_filename, writeout=False):
    # index_filename = f"{date}_segmentation_pointclouds_index"
    json_index = {}

    #sometimes theres' just no file for that date
    try:
        index_file = open(index_filename,"r").read()
        # for each line in the file make an element in the dictionary by plantname
        lines = index_file.split("\n")
        for line in lines:
            if "ply" in line:
                # get size
                clean_line = re.sub("\s+"," ",line)
                parts = clean_line.split(" ")
    #             print("\n".join([f"{i},{p}" for i,p in enumerate(parts)]))
                block = int(parts[1].replace(":",""))
                file_size = int(parts[4])
                path = Path(parts[7])
                name = path.parent.stem
                json_index.setdefault(name,[]).append({"block":block,"file_size":file_size,"path":str(path),"filename":path.stem})
        if writeout:
            print('Writing to file.')
            fn  = f"{index_filename}.json"
            with open(fn,"w") as phile:
                phile.write(json.dumps(json_index))
        
    except Exception as e:
        print(e)
        print("date missing tar index", date)
    return json_index


#-------------------------------------------------------------------------------
def make_range_request(url,start,end):
    range_header = {'Range':f"bytes={start}-{end}"}
    res = requests.get(url,headers=range_header)
    return res.content


#-------------------------------------------------------------------------------
def get_dict():
    '''
    Provides notation for CyVerse directories. 
    
    Input:
        - NA
    Output: 
        - A dictionary containing season, level, and sensor notations which will be used to query the CyVerse Data Store. 
    '''

    irods_dict = {
        'server_path': '/iplant/home/shared/phytooracle/',

        'season': {
            '10': 'season_10_lettuce_yr_2020',
            '11': 'season_11_sorghum_yr_2020',
            '12': 'season_12_sorghum_soybean_sunflower_tepary_yr_2021',
            '13': 'season_13_lettuce_yr_2022',
            '14': 'season_14_sorghum_yr_2022',
            '15': 'season_15_lettuce_yr_2022'
        },

        'level': {
            '0': 'level_0', 
            '1': 'level_1',
            '2': 'level_2',
            '3': 'level_3',
            '4': 'level_4'
        },

        'sensor': {
            'FLIR': 'flirIrCamera',
            'PS2': 'ps2Top',
            'RGB': 'stereoTop',
            '3D': 'scanner3DTop',
            'ENV': 'EnvironmentLogger'
        }
    }

    return irods_dict


#-------------------------------------------------------------------------------
def get_file_list(data_path, sequence):
    '''
    Using the dictionary containing season, level, and sensor notations, this function finds all files matching the season, 
    level, and sensor paths, as well as an identifying sequence such as %.tar.gz. The % is similar to Linux's wild card "*"
    
    Input:
        - data_path: The CyVerse Data Store path created from dictionary
        - sequence: An identifying sequence, such as "%.tar.gz". The "%" character is similar to Linux's wild card "*" character.
    Output: 
        - List of files matching the season, level, sensor, and sequence
    '''
    result = sp.run(f'ilocate {os.path.join(data_path, "%", f"{sequence}")}', stdout=sp.PIPE, shell=True)
    files = result.stdout.decode('utf-8').split('\n')

    return files


#-------------------------------------------------------------------------------
def download_files(item, out_path):
    '''
    Uses iRODS to access the CyVerse Data Store. The function downloads data and extracts contents from ".tar" and "tar.gz" files if applicable.
    
    Input:
        - item: The list of CyVerse file paths to download locally.
        - out_path: Output directory where data will be saved. 
    Output: 
        - Downloaded and extracted data within the specified output directory 
    '''
        
    os.chdir(out_path)

    if not 'deprecated' in item:

        try:
            item = os.path.normpath(item)

            try:

                match_str = re.search(r'\d{4}-\d{2}-\d{2}__\d{2}-\d{2}-\d{2}-\d{3}', item)
                date = match_str.group()
                # date = datetime.strptime(match_str.group(), '%Y-%m-%d').date()
            except:
                match_str = re.search(r'\d{4}-\d{2}-\d{2}', item)
                date = datetime.strptime(match_str.group(), '%Y-%m-%d').date()
                date = str(date)

            print(f"Found item {item}.")

            if not os.path.isdir(date):
                print(f"Making directory {date}.")
                os.makedirs(date)


            if '.tar.gz' in item: 
                print(f"Downloading {item}.")
                sp.call(f'iget -KPVT {item}', shell=True)

                print(f"Extracting {item}.")
                ret = sp.call(f'tar -xzvf {os.path.basename(item)} -C {date}', shell=True)
                # ret = sp.call(f'tar -c --use-compress-program=pigz -f {os.path.basename(item)}', shell=True) #-C {date} 

                if ret != 0:
                    print(f"Reattempting to extract {item}.")
                    sp.call(f'tar -xvf {os.path.basename(item)} -C {date}', shell=True)

                sp.call(f'rm {os.path.basename(item)}', shell=True)

            elif '.tar' in item:
                print(f"Downloading {item}.")
                sp.call(f'iget -KPVT {item}', shell=True)
                
                print(f"Extracting {item}.")
                sp.call(f'tar -xvf {os.path.basename(item)} -C {date}', shell=True)
                sp.call(f'rm {os.path.basename(item)}', shell=True)

            else:
                os.chdir(date)
                sp.call(f'iget -KPVT {item}', shell=True)
            
        except:
            pass

        
#-------------------------------------------------------------------------------
def download_data(crop, season, level, sensor, sequence, cwd, outdir, download=True):
    '''
    Recursively runs `download_files` to download all data into a single output directory specified by the user.
    
    Input:
        - crop: The name of the crop data to download, either "lettuce" or "sorghum"
        - season: The season numer to download, either 14, 15, or 16
        - level: The level of data to download, either 0, 1, 2
        - sensor: The name of the sensor to download, either RGB, FLIR, PS2, or 3D
        - sequence: The identifying sequence to download, such as ".tar" or ".tar.gz"
        - cwd: The current working directory
        - outdir: The output directory
        - download: Boolean value to specify whether to download data (True) or not (False)

    Output: 
        - Downloaded and extracted data within the specified output directory 
    '''

    try:

        irods_dict = get_dict()
        # Create iRODS path from components. 
        data_path = os.path.join(irods_dict['server_path'], irods_dict['season'][season], irods_dict['level'][level], irods_dict['sensor'][sensor])
        if crop != "NA":
            data_path = os.path.join(irods_dict['server_path'], irods_dict['season'][season], irods_dict['level'][level], irods_dict['sensor'][sensor], crop)
        # Get list of all files that match a character sequence.
        print(f'Searching for files matching "{os.path.join(data_path, sequence)}". Note: This process may take 1-5 minutes.')
        files = get_file_list(data_path, sequence)
        print('Matches obtained.')

        # Prepare to download data.
        out_path = os.path.join(outdir, irods_dict['season'][season], irods_dict['sensor'][sensor])
        if not os.path.isdir(out_path):
            os.makedirs(out_path)

        if download:
            os.chdir(out_path)

            # Download files.
            for item in files: 
                print(f'Downloading {item}.')
                download_files(item=item, out_path=os.path.join(cwd, out_path))
                
            os.chdir(cwd)
        
        return out_path
    
    except Exception as e:
        # code to handle the exception
        print(f"An error occurred while downloading data: {e}")


# --------------------------------------------------
def find_files(directory, pattern):
    result = []
    for root, dirs, files in os.walk(directory):
        for file in files:
            if file.endswith(pattern):
                result.append(os.path.join(root, file))
    return result


# --------------------------------------------------
def search_files(files, substrings):
    result = []
    for file in files:
        if any(substring in file for substring in substrings):
            result.append(file)
    return result


#-------------------------------------------------------------------------------
def download_plant_by_index(plant_name):
    args = get_args()
    print(plant_name)
    folder = os.path.join(args.out_dir, '_'.join([plant_name, 'timeseries']))
    # folder = Path(f"plant_plys/{plant_name}_timeseries")
    # folder.mkdir(exist_ok=True,parents=True)
    os.makedirs(folder, exist_ok=True)
    # go through the dates
    for date in all_dates:
        # print(date)
        irods_dict = get_dict()
        season_path = irods_dict['season'][args.season]
        level = irods_dict['level'][args.level]

        # construct a url for the date
        if season_path == 'season_10_lettuce_yr_2020':
            ipath = f"https://data.cyverse.org/dav-anon/iplant/commons/community_released/phytooracle/{season_path}/{level}/scanner3DTop/{date}/individual_plants_out/{date}_segmentation_pointclouds.tar"
        else:
            ipath = f"https://data.cyverse.org/dav-anon/iplant/commons/community_released/phytooracle/{season_path}/{level}/scanner3DTop/{args.crop}/{date}/individual_plants_out/{date}_segmentation_pointclouds.tar"
        # print(ipath)
        # look through all_dates for the plant data
        # index by plant
        index_date = all_dates[date]
        plant_files = index_date.get(plant_name,-1)
        if plant_files != -1:
            print("\t HIT")
#             print(json.dumps(plant_files,indent=2))
            # lets go get all the plys on the names we care about
            for ply in plant_files:
                res = f'{folder}/{date}_{ply["filename"]}.ply'
                if not "final" in ply["filename"]:
                    continue
                if Path(res).exists():
                    print("already downloaded",ply["filename"])
                    continue
                # if we don't add a 512 to the start we get the tar header also
                start = ply["block"]*512 + 512
                end = start+ ply["file_size"]
                print(ipath)
                ply_buffer = make_range_request(ipath,start,end)

                with open(res,"wb") as phile:
                    phile.write(ply_buffer)


# --------------------------------------------------
def main():
    """Make a jazz noise here"""
    args = get_args()

    print(args)

    # Create output directory
    os.makedirs(args.out_dir, exist_ok=True)

    # Get the working directory
    wd = os.getcwd()
    
    # Download data
    if not os.path.isdir("index_files"):

        os.makedirs("index_files", exist_ok=True)
        download_data(
                    crop = args.crop,
                    season = args.season,
                    level = args.level,
                    sensor = args.instrument,
                    sequence = '%/individual_plants_out/%_segmentation_pointclouds_index',
                    cwd = wd,
                    outdir = "index_files"
                    )
    
    print("donwnloaded")
    files = find_files("index_files", "_segmentation_pointclouds_index")
    matching_files = search_files(files, args.final_date)

    # here I was getting the plants that made it all the way through the season, so I checked the last date of the season to get the plant names that are still in the field at that time
    # here you would put in your last index file
    last_index = ""
    for matching_file in matching_files:
        with open(matching_file, 'r') as f:
            last_index += f.read() + "\n"

    # this looks through the index file and takes the last part of each line which just contains the file name
    paths = [f.split(" ")[-1] for f in last_index.split("\n")]
    #we only want the names in my case if the final.ply is part of the name
    plants = [f.split("/")[-2] for f in paths if "final.ply" in f]
    
    if args.genotype:
        filtered =[]
        for p in plants:
            for genotype in args.genotype:
                if genotype.strip() in p:
                    filtered.append(p)

        plants = filtered

    # figure out how many plants we have already generated timeseries representations for so we don't have to go back to these ones
    try:
        prev_plant_names = [ f.replace("_timeseries","") for f in os.listdir("./plant_plys/") if "_timeseries" if str(f)]
    except: 
        prev_plant_names = []

    # use set arithmetic to remove the plants that we've run already, I guess I wasn't making use of the prev_plant_names but they would be used instead of the [] in the - set()
    plant_names = list(set(plants) - set(prev_plant_names))

    print(len(plant_names))
    # per index file get the date so we can make a dictionary containing all the dates
    index_files = sorted(files)
    print(index_files)
    global all_dates
    all_dates ={}
    for ifile in index_files:
        if not args.season == '10':
            match = re.search(r'\d{4}-\d{2}-\d{2}__\d{2}-\d{2}-\d{2}-\d{3}', ifile.split(os.sep)[-1])
        else:
            match = re.search(r'\d{4}-\d{2}-\d{2}', ifile.split(os.sep)[-1])
        
        if match:
            date = match.group()

        if not args.season == '10':
            date = '_'.join([date, args.crop])
        
        print(date)
    #     date = str(ifile).split("_")[0]
        date_json = json_index(date, ifile)
        all_dates[date] = date_json

        print(all_dates)

    # we can then start up a mapping between those cores and the total list of plant names, which we then map to the download_plany_by_index function
    # this allows us to tackle the independent download tasks in parallel with however many cores we have available.
    with mp.Pool(mp.cpu_count()) as pool:
       pool.map(download_plant_by_index,plant_names)


# --------------------------------------------------
if __name__ == '__main__':
    main()
