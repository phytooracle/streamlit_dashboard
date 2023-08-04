import os
from pathlib import Path
import requests
import json
import re


class Fetcher:
    def __init__(self, out_dir, season, level, date, crop, index_filename):
        self.out_dir = out_dir
        self.season = season
        self.level = level
        self.date = date
        self.crop = crop
        self.index_date = self.json_index(date, index_filename)
        self.irods_dict = {
            "server_path": "/iplant/home/shared/phytooracle/",
            "season": {
                "Season 10": "season_10_lettuce_yr_2020",
                "Season 11": "season_11_sorghum_yr_2020",
                "Season 12": "season_12_sorghum_soybean_sunflower_tepary_yr_2021",
                "Season 13": "season_13_lettuce_yr_2022",
                "Season 14": "season_14_sorghum_yr_2022",
                "Season 15": "season_15_lettuce_yr_2022",
            },
        }

    # Maybe move this over to dashboard since file must be downloaded there
    def json_index(self, date, index_filename, writeout=True):  # , writeout=False
        # index_filename = f"{date}_segmentation_pointclouds_index"
        json_index = {}

        # sometimes theres' just no file for that date
        try:
            index_file = open(index_filename, "r").read()
            # for each line in the file make an element in the dictionary by plantname
            lines = index_file.split("\n")
            for line in lines:
                if "ply" in line:
                    # get size
                    clean_line = re.sub("\s+", " ", line)
                    parts = clean_line.split(" ")
                    #             print("\n".join([f"{i},{p}" for i,p in enumerate(parts)]))
                    block = int(parts[1].replace(":", ""))
                    file_size = int(parts[4])
                    path = Path(parts[7])
                    name = path.parent.stem
                    json_index.setdefault(name, []).append(
                        {
                            "block": block,
                            "file_size": file_size,
                            "path": str(path),
                            "filename": path.stem,
                        }
                    )
            if writeout:
                print("Writing to file.")
                index_filename = index_filename.split(".")[0]
                fn = f"{index_filename}.json"
                with open(fn, "w") as phile:
                    phile.write(json.dumps(json_index))

        except Exception as e:
            print(e)
            print("date missing tar index", date)
        return json_index

    def make_range_request(self, url, start, end):
        range_header = {"Range": f"bytes={start}-{end}"}
        res = requests.get(url, headers=range_header)
        return res.content

    # ask emmanuel about all_dates and what it is
    # all dates currently not instatiated
    # INCOMPLETE
    def download_plant_by_index(self, plant_name):
        folder = os.path.join(self.out_dir, "_".join([plant_name, "timeseries"]))
        # folder = Path(f"plant_plys/{plant_name}_timeseries")
        # folder.mkdir(exist_ok=True,parents=True)
        os.makedirs(folder, exist_ok=True)
        # go through the dates

        # season_path = self.irods_dict['season'][self.season]

        # construct a url for the date
        if self.season == "season_10_lettuce_yr_2020":
            ipath = f"https://data.cyverse.org/dav-anon/iplant/commons/community_released/phytooracle/{self.season}/{self.level}/scanner3DTop/{self.date}/individual_plants_out/{self.date}_segmentation_pointclouds.tar"
        else:
            ipath = f"https://data.cyverse.org/dav-anon/iplant/commons/community_released/phytooracle/{self.season}/{self.level}/scanner3DTop/{self.crop}/{self.date}/individual_plants_out/{self.date}_segmentation_pointclouds.tar"
        # print(ipath)
        # look through all_dates for the plant data
        # index by plant

        plant_files = self.index_date.get(plant_name, -1)
        if plant_files != -1:
            # print("\t HIT")
            #             print(json.dumps(plant_files,indent=2))
            # lets go get all the plys on the names we care about
            for ply in plant_files:
                res = f'{folder}/{self.date}_{ply["filename"]}.ply'
                if not "final" in ply["filename"]:
                    continue
                if Path(res).exists():
                    print("already downloaded", ply["filename"])
                    continue
                # if we don't add a 512 to the start we get the tar header also
                start = ply["block"] * 512 + 512
                end = start + ply["file_size"]
                # print(ipath)
                ply_buffer = self.make_range_request(ipath, start, end)

                # print(ply_buffer)
                with open(res, "wb") as phile:
                    phile.write(ply_buffer)

                return res

            # have to add return for either the data itself or the local location of the downloaded file
