import os
from pathlib import Path
import requests

class Fetcher:

    def __init__(self, out_dir, season, level):
        self.out_dir = out_dir
        self.season = season
        self.level = level
        self.irods_dict = {
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

    def make_range_request(self, url,start,end):
        range_header = {'Range':f"bytes={start}-{end}"}
        res = requests.get(url,headers=range_header)
        return res.content
    
    
    #ask emmanuel about all_dates and what it is 
    # all dates currently not instatiated 
    # INCOMPLETE   
    def download_plant_by_index(self, plant_name):
        
        folder = os.path.join(self.out_dir, '_'.join([plant_name, 'timeseries']))
        # folder = Path(f"plant_plys/{plant_name}_timeseries")
        # folder.mkdir(exist_ok=True,parents=True)
        os.makedirs(folder, exist_ok=True)
        # go through the dates
        for date in all_dates: #INCOMPLETE
            # print(date)
            season_path = self.irods_dict['season'][self.season]
            level = self.irods_dict['level'][self.level]

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
                    ply_buffer = self.make_range_request(ipath,start,end)

                    with open(res,"wb") as phile:
                        phile.write(ply_buffer)

            #INCOMPLETE
            return None
            # have to add return for either the data itself or the local location of the downloaded file