"""
Author: Aditya K. and Emily C. 
Purpose: A dasboard to visualize and download the results of pipeline processing. 
   Deployed on Streamlit Cloud. 

Some Helpful Hints: 
If something is not working fine, please take a look at the paths and
the regexs (especially them! ) first. If they look fine, don't change them!
"""
from irods.session import iRODSSession
from datetime import datetime, timedelta
import pandas as pd
import streamlit as st
import plotly.express as px
import re
import os
import tarfile
import glob
import shutil  # remove filled directory to manage space
import json
import fetch_ipc as fipc
import streamlit.components.v1 as components
import open3d as o3d
import numpy as np
from st_aggrid import AgGrid, GridOptionsBuilder


@st.cache_data
def convert_df(df):
    # Cache the conversion to prevent computation on every rerun
    return df.to_csv(index=False).encode("utf-8")


def get_seasons(_session):
    """A method to download the list of seasons and update them through Cyverse.

    Args:
      - _session (irodsSession): A _session object that allows the program to query data
      from the Cyverse datastore.

    Returns:
      - seasons (dict) : A dictionary that looks like this ->
      {'The season name to be displayed': 'Actual name we got'}
    """
    seasons = {}
    try:
        root_collection = _session.collections.get("/iplant/home/shared/phytooracle")
    except:
        st.write("No data present on Cyverse datastore for any season. ")
        return seasons
    else:
        for directory in root_collection.subcollections:
            if re.search("season*", directory.name):
                seasons["Season " + directory.name.split("_")[1]] = directory.name
        return seasons


@st.cache_data
def get_sensors(_session, season):
    """A method to download the sensors available for a given season
    and update them through Cyverse.

    Args:
      - _session (irodsSession): A _session object that allows the program to query data
      from the Cyverse datastore.
      - season (string): The actual name that appears on cyverse for the selected season

    Returns:
      - sensors (list): A list of the available sensors.
    """
    sensors = []
    try:
        sensor_collection = _session.collections.get(
            f"/iplant/home/shared/phytooracle/{season}/level_1"
        )
    except:
        st.write("None of the sensor data has been processed yet for this season. ")
        return sensors
    else:
        for sensor in sensor_collection.subcollections:
            if (
                not re.search("deprecated", sensor.name)
                and not re.search("environmentlogger", sensor.name, re.IGNORECASE)
                and not re.search("drone", sensor.name, re.IGNORECASE)
            ):
                sensors.append(sensor.name)
        return sensors


@st.cache_data
def get_crops(_session, season, sensor, alt_layout):
    """A method to download the crops processed for a given season/sensor combo.
    and update them through Cyverse.

    Args:
      - _session (irodsSession): A _session object that allows the program to query data
      from the Cyverse datastore.
      - season (string): The actual name that appears on cyverse for the selected season
      - sensor (string): The name of the sensor to be used.
      - alt_layout (boolean): A bool to indicate if the user selected a season that
      has an alternate data organization layout.

    Returns:
      - crops: A list of crops that were processed.
    """
    crops = []
    # Alt layout don't have a wrapper folder with the name of the crop
    if not alt_layout:
        try:
            season_crop_collection = _session.collections.get(
                f"/iplant/home/shared/phytooracle/{season}/level_1/{sensor}"
            )
        except:
            st.write("No data for this sensor in this season. ")
            return crops
        else:
            for crop in season_crop_collection.subcollections:
                if not re.search("dep*", crop.name) and not re.search(
                    "[0-9][0-9a-zA-z]*", crop.name
                ):
                    crops.append(crop.name)
            return crops
    return crops


@st.cache_resource
def display_processing_info(_session, seasons, selected_season, sensors, crop):
    info_sec = st.container()
    info_sec.divider()
    info_sec.header(":blue[Processing Information]")
    info_sec.markdown(
        f"Here is the status of processing for different sensor data for :red[{selected_season}] **({crop})**"
    )
    season = seasons[selected_season]
    season_folders = _session.collections.get(
        f"/iplant/home/shared/phytooracle/{season}/"
    )
    levels = []
    for folder in season_folders.subcollections:
        if re.search("level_[0-9]+", folder.name):
            levels.append(folder.name)
    sensor_data = ""
    total_files_ct_all = 0
    processed_files_ct_all = 0
    sensor_df = pd.DataFrame({"sensor": [], "processed": [], "unprocessed": []})
    for sensor in sensors:
        for level in sorted(levels, reverse=True):
            try:
                _session.collections.get(
                    f"/iplant/home/shared/phytooracle/{season}/{level}/{sensor}"
                )
            except:
                continue
            else:
                level_no = int(level.split("_")[1])
                if sensor != "stereoTop" or level_no <= 1:
                    # for crop in crops:
                    level_0_file_ct = get_and_count_files_in_folder(
                        _session, season, sensor, crop, "level_0"
                    )
                    level_X_file_ct = get_and_count_files_in_folder(
                        _session, season, sensor, crop, level
                    )
                    total_files_ct_all += level_0_file_ct
                    processed_files_ct_all += level_X_file_ct
                    new_row = pd.DataFrame(
                        {
                            "sensor": [sensor],
                            "processed": [level_X_file_ct],
                            "unprocessed": [level_0_file_ct - level_X_file_ct],
                        }
                    )
                    sensor_df = pd.concat([sensor_df, new_row], ignore_index=True)
                    percentage_processed = (level_X_file_ct / level_0_file_ct) * 100
                else:
                    # A single common file created for RGB higher processing, so if there is a folder
                    # then everything has been processed
                    level_0_file_ct = get_and_count_files_in_folder(
                        _session, season, sensor, crop, "level_0"
                    )
                    new_row = pd.DataFrame(
                        {
                            "sensor": [sensor],
                            "processed": [level_X_file_ct],
                            "unprocessed": [0],
                        }
                    )
                    sensor_df = pd.concat([sensor_df, new_row], ignore_index=True)
                    percentage_processed = 100
                sensor_data += f"**:green[{sensor.upper()}]** processed to **Level {level_no}** (:orange[{round(percentage_processed, 2)}%]),  "
                break
    info_sec.markdown(sensor_data[:-2])
    cumulative_stats, sensor_stats = info_sec.columns(2)
    entire_df = pd.DataFrame(
        {
            "names": ["Processed Files", "Unprocessed Files"],
            "values": [
                processed_files_ct_all,
                total_files_ct_all - processed_files_ct_all,
            ],
        }
    )
    full_stats = px.pie(
        entire_df,
        values="values",
        names="names",
        hole=0.9,
        color_discrete_sequence=["#2E8B57", "#3CB371"],
        title="Cumulative Statistics",
    )
    full_stats.update_layout(
        uniformtext_minsize=12,
        uniformtext_mode="hide",
        annotations=[
            dict(
                text=f"{total_files_ct_all - processed_files_ct_all} files left",
                x=0.5,
                y=0.5,
                font_size=30,
                showarrow=False,
                # font_color="#1c1c1c",
            )
        ],
    )
    cumulative_stats.plotly_chart(full_stats, use_container_width=True)
    sensor_chart = px.bar(
        sensor_df,
        x="sensor",
        y=["processed", "unprocessed"],
        labels={"value": "No. of files", "variable": "Processing State"},
        title="Sensor Specific Data",
        color_discrete_sequence=["#2E8B57", "#3CB371"],
    )
    sensor_stats.plotly_chart(sensor_chart, use_container_width=True)
    info_sec.divider()


def get_and_count_files_in_folder(_session, season, sensor, crop, level):
    count = 0
    if level != "level_0":
        file_collection = _session.collections.get(
            f"/iplant/home/shared/phytooracle/{season}/{level}/{sensor}/{crop}"
        )
        for folder in file_collection.subcollections:
            if not re.search("dep*", folder.name) and re.search("\d+\s*", folder.name):
                count += 1
    else:
        file_collection = _session.collections.get(
            f"/iplant/home/shared/phytooracle/{season}/{level}/{sensor}"
        )
        for folder in file_collection.data_objects:
            if not re.search("dep*", folder.name) and re.search(crop, folder.name):
                count += 1
    return count


@st.cache_data
def get_dates(_session, season, sensor, crop):
    """A method to get a list of dates processed for a given
    season/sensor/crop combo from cyverse data store.

    Args:
      - _session (irodsSession): A _session object that allows the program to query data
      from the Cyverse datastore.
      - season (string): The actual name that appears on cyverse for the selected season
      - sensor (string): The name of the sensor to be used.
      - crop (string): The name of the crop to be accessed.

    Returns:
      - dates (dict) : A dictionary that looks like this ->
      {'The date to be displayed (YYYY-MM-DD)': 'Actual date we got (contains time)'}
    """
    dates = {}
    try:
        if crop != "":
            season_date_collection = _session.collections.get(
                f"/iplant/home/shared/phytooracle/{season}/level_1/{sensor}/{crop}/"
            )
        else:
            season_date_collection = _session.collections.get(
                f"/iplant/home/shared/phytooracle/{season}/level_1/{sensor}/"
            )
    except:
        st.write(
            f"This season's crop has not been processed yet for {sensor} sensor. Check back later."
        )
        return dates
    else:
        for directory in season_date_collection.subcollections:
            if not re.search("dep", directory.name) and re.search(
                "[0-9][0-9a-zA-z]*", directory.name
            ):
                dates[directory.name.split("_")[0]] = directory.name
        return dates


@st.cache_data
def get_plant_detection_csv_path(
    _session, season, sensor, crop, dates, selected_date, alt_layout
):
    """A method to get the location of the tarball that should
    have the plant detection csv. Only RGB and FLIR sensors have this

    Args:
      - _session (irodsSession): A _session object that allows the program to query data
      from the Cyverse datastore.
      - season (string): The actual name that appears on cyverse for the selected season
      - sensor (string): The name of the sensor to be used.
      - alt_layout (boolean): A bool to indicate if the user selected a season that
      has an alternate data organization layout.
      - dates (dictionary): The dictionary returned by the get_dates() method.
      - selected_date (string): The date selected by the user (no time)

    Returns:
      - string: path
    """
    if re.search("3d", sensor, re.IGNORECASE):
        # Download CSV from the RGB sensor level 1
        dates_RGB = get_dates(_session, season, "stereoTop", crop)
        date_RGB = ""
        date_3D_obj = datetime.strptime(selected_date, "%Y-%m-%d")
        for date_id in dates_RGB.keys():
            dte_obj = datetime.strptime(date_id, "%Y-%m-%d")
            # to check neighboring dates in case of no exact match
            if (date_3D_obj == dte_obj) or (
                date_3D_obj == dte_obj + timedelta(days=1)
                or date_3D_obj == dte_obj - timedelta(days=1)
            ):
                date_RGB = date_id
                break
        if date_RGB == "":
            st.write(
                f"No Plant Detection CSV is present for this sensor on this date. ({selected_date})"
            )
            return ""
        date = dates_RGB[date_RGB]
        sensor = "stereoTop"
    else:
        date = dates[selected_date]
    if alt_layout:
        date_directory_path = (
            f"/iplant/home/shared/phytooracle/{season}/level_1/{sensor}/{date}"
        )
    else:
        date_directory_path = (
            f"/iplant/home/shared/phytooracle/"
            f"{season}/level_1/"
            f"{sensor}/{crop}/{date}"
        )
    date_directory = _session.collections.get(date_directory_path)
    # To go through the processed files for the date to finde the plant detection zip
    for files in date_directory.data_objects:
        if sensor != "ps2Top":
            if "detect_out" in files.name:
                return f"{date_directory_path}/{files.name}"
        else:
            if "aggregation_out" in files.name:
                return f"{date_directory_path}/{files.name}"
    st.write("No Plant Detection CSV is present for this sensor on this date. ")
    return ""


@st.cache_data
def download_fieldbook(_session, season):
    """Download the fieldbook for the specified season
    if it is not already in the cache.

    Args:
      - _session (irodsSession): A _session object that allows the program to query data
      from the Cyverse datastore.
      season (string): The actual name that appears on cyverse for the selected season

    Returns:
      - string: Name of the fieldbook file, nothing if nothing is found
    """
    season_file_collection = _session.collections.get(
        f"/iplant/home/shared/phytooracle/{season}"
    )
    season_no = season.split("_")[1]
    for file in season_file_collection.data_objects:
        if re.search("field*book", file.name, re.IGNORECASE) or re.search(
            "book", file.name, re.IGNORECASE
        ):
            file_type = file.name.split(".")[1]
            if not (
                os.path.exists(f"field_books/season_{season_no}_fieldbook.{file_type}")
            ):
                if not os.path.exists("field_books"):
                    os.makedirs("field_books")
                _session.data_objects.get(
                    f"/iplant/home/shared/phytooracle/{season}/{file.name}",
                    f"field_books/season_{season_no}_fieldbook.{file_type}",
                )
            return f"season_{season_no}_fieldbook.{file_type}"
    return ""


@st.cache_data
def download_plant_detection_csv(_session, local_file_name, plant_detection_csv_path):
    """Download and extract the plant detection csv from the file name
    Args:
      - _session (irodsSession): A _session object that allows the program to query data
      from the Cyverse datastore.
      - local_file_name (string): Name of the plant detection CSV if stored locally
      - plant_detection_csv_path (string): Path on cyverse
    """
    if not (os.path.exists(f"detect_out/{local_file_name}")):
        _session.data_objects.get(
            plant_detection_csv_path, "local_file_delete.tar", force=True
        )
        with tarfile.open("local_file_delete.tar", "r") as tar:
            tar.extractall()
        os.remove("local_file_delete.tar")


@st.cache_resource
def create_file_fetcher(_session, season, date, crop):
    closest_date = find_closest_date(_session, season, date, crop)
    if closest_date is None:
        # There are no level 2 point clouds for the chosen date or any date near it
        file_fetcher = None
    else:
        cyverse_idx_path = f"/iplant/home/shared/phytooracle/{season}/level_2/scanner3DTop/{crop}/{closest_date}/individual_plants_out/{closest_date}_segmentation_pointclouds_index"
        local_idx_path = (
            f"visualization/{closest_date}_segmentation_pointclouds_index.txt"
        )
        # First check if we already have the index files downloaded
        if not os.path.exists(local_idx_path):
            if not os.path.exists("visualization"):
                os.makedirs("visualization")
            _session.data_objects.get(cyverse_idx_path, local_idx_path)
        file_fetcher = fipc.Fetcher(
            "individually_called_point_clouds",
            season,
            "level_2",
            closest_date,
            crop,
            local_idx_path,
        )
    return file_fetcher


def data_analysis(
    _session, season, plant_detect_df, field_book_name, sensor, crop, date, layout
):
    """Begin Analyzing the plant detection data. Start by making the fieldbook
    dataframe.
    Args:
      - _session (irodsSession): A _session object that allows the program to query data
      from the Cyverse datastore.
      - season (string): The actual name that appears on cyverse for the selected season
      - plant_detect_df (dataframe): Pandas dataframe made using the plant detection CSV
      - field_book_name (string): Name of the fielbook file.
      - sensor (string): The name of the sensor to be used.
      - layout (boolean): A bool to indicate if the user selected a season that
      has an alternate data organization layout.
      - crop (string): Selected crop.
      - date (string): Specified date.
    """

    # make field book dataframe based on its extension
    if field_book_name.split(".")[1] == "xlsx":
        try:
            field_book_df = pd.read_excel(
                io=f"field_books/{field_book_name}",
                sheet_name="fieldbook",
                engine="openpyxl",
            )
        except:
            field_book_df = pd.read_excel(
                io=f"field_books/{field_book_name}",
                sheet_name="Fieldbook",
                engine="openpyxl",
            )
    elif field_book_name.split(".")[1] == "csv":
        field_book_df = pd.read_csv(f"field_books/{field_book_name}")
    else:
        st.write(
            f"Can't deal with files of this extension yet for the file {field_book_name}."
        )
        st.write("Please contact the Phytooracle staff")
        return
    plant_detect_df = plant_detect_df.rename(columns={"Plot": "plot"})
    result = plant_detect_df.merge(field_book_df, on="plot")
    copy = extra_processing(_session, season, result, sensor, crop, date, layout)
    # This is NOT CASE SENSITIVE. KEEP THIS IN MIND. CHANGE IF NECESSARY
    if not (copy.empty and ("stereo" in sensor or "flir" in sensor)):
        result = copy
    if not result.empty:
        # To drop duplicate genotype columns
        result = result.drop("genotype_y", axis=1, errors="ignore")
        result = result.rename(columns={"genotype_x": "genotype"}, errors="ignore")
        # to drop min/max_x/y/z
        result.drop(list(result.filter(regex="min_?|max_?")), axis=1, inplace=True)
        # to drop empty index col
        result.drop(
            result.columns[result.columns.str.contains("unnamed", case=False)],
            axis=1,
            inplace=True,
        )
        result.to_csv(f"{sensor}_{season}_{crop}_{date}.csv", index=False)
        _session.data_objects.put(
            f"{sensor}_{season}_{crop}_{date}.csv",
            f"/iplant/home/shared/phytooracle/dashboard_cache/{sensor}/combined_data/{season}_{date}_all.csv",
        )
        os.remove(f"{sensor}_{season}_{crop}_{date}.csv")
        # GETTING POINT CLOUDS NOW
        file_fetcher = create_file_fetcher(_session, season, date, crop)
        return file_fetcher, result
        # create_filter(file_fetcher, combined_data=result, sensor=sensor)
    else:
        # result dataframe is empty, allow the users to download the plant detection csv and fieldbook csv
        st.subheader("Some Error occured (might be a merge issue).")
        st.write(
            "Below are the links to download the Plant Detection and the Fieldbook CSV"
        )
        st.download_button(
            label="Download Plant Detection CSV",
            data=convert_df(plant_detect_df),
            file_name=f"{date}_plant_detect_out.csv",
            mime="text/csv",
            key="data_frame_empty_download_csv"
        )
        st.download_button(
            label="Download Fieldbook Data",
            data=convert_df(field_book_df),
            file_name=f"{season}_fieldbook.csv",
            mime="text/csv",
            key="data_frame_empty_download_fieldbook"
        )
        return None, None


@st.cache_resource
def find_closest_date(_session, season, actual_date, crop):
    only_date = actual_date.split("_")[0]
    actual_date_obj = datetime.strptime(only_date, "%Y-%m-%d")
    # get all the dates of the 3d data for the selected season
    try:
        collection = _session.collections.get(
            f"/iplant/home/shared/phytooracle/{season}/level_2/scanner3DTop/{crop}"
        )
    except:
        return None
    else:
        for unprocessed_date in collection.subcollections:
            processed_date = unprocessed_date.name.split("_")[0]
            if only_date == processed_date:
                return unprocessed_date.name
        for date_folder in collection.subcollections:
            potential_date = date_folder.name.split("_")[0]
            if potential_date[0].isnumeric():
                potential_date_obj = datetime.strptime(potential_date, "%Y-%m-%d")
                if (potential_date_obj == actual_date_obj + timedelta(days=1)) or (
                    potential_date_obj == actual_date_obj - timedelta(days=1)
                ):
                    return date_folder.name
        else:
            return None


@st.cache_resource
def extra_processing(_session, season, combined_df, sensor, crop, date, alt_layout):
    """This part deals with downloading files and doing extra work for the
    3D and PSII sensors, that don't have geolocation data stored in one file.

    Args:
      - _session (irodsSession): A _session object that allows the program to query data
      from the Cyverse datastore.
      - season (string): The actual name that appears on cyverse for the selected season
      - plant_detect_df (dataframe): Pandas dataframe made using the plant detection CSV
      - combined_df: Pandas dataframe that is basically fieldbook + plant detection
      - sensor (string): The name of the sensor to be used.
      - alt_layout (boolean): A bool to indicate if the user selected a season that
      has an alternate data organization layout.
      - crop (string): Selected crop.
      - date (string): Specified date.
    """

    season_no = season.split("_")[1]
    if "3D" in sensor:
        try:
            download_extra_3D_data(_session, season, season_no, sensor, crop, date)
            download_plant_clustering_csv(_session, season, season_no)
            ind_plant_df = combine_all_csv(
                "3d_volumes_entropy_v009", sensor, crop, date
            )
            plant_clustering_df = pd.read_csv(
                f"plant_clustering/season_{season_no}_clustering.csv"
            ).loc[:, ["plant_name", "lat", "lon"]]
            # Taking very long time - 3 mins (try merging plant_clustering with ind first)
            ind_plant_df = ind_plant_df.merge(plant_clustering_df, on="plant_name")
            combined_df = combined_df.merge(ind_plant_df, on=["lat", "lon"])
            return combined_df
        except Exception as e:
            st.write(
                f"The 3D scans for this season were not processed to the level required for any visuals."
                f"Please try any other sensor/season. "
            )
            return pd.DataFrame()
    # for the other sensor (stereoTop, FLIR and PSII)
    else:
        try:
            download_plant_clustering_csv(_session, season, season_no)
        except Exception as e:
            st.write(
                f"Couldn't find the plant clustering CSV file for this season. Contact Phytooracle staff."
            )
            return pd.DataFrame()
        else:
            if re.search("ps2", sensor, re.IGNORECASE):
                plant_clustering_df = pd.read_csv(
                    f"plant_clustering/season_{season_no}_clustering.csv"
                ).loc[:, ["plant_name", "plot", "genotype", "lat", "lon"]]
                st.dataframe(plant_clustering_df)
                st.dataframe(combined_df)
                combined_df = combined_df.merge(
                    plant_clustering_df, on=["plot", "genotype"]
                )
            else:
                plant_clustering_df = pd.read_csv(
                    f"plant_clustering/season_{season_no}_clustering.csv"
                ).loc[:, ["plant_name", "lat", "lon"]]
                combined_df = combined_df.merge(plant_clustering_df, on=["lat", "lon"])
            return combined_df


@st.cache_data
def download_extra_3D_data(_session, season, season_no, sensor, crop, date):
    if not (os.path.exists(f"3d_volumes_entropy_v009")):
        collection = _session.collections.get(
            f"/iplant/home/shared/phytooracle/{season}/level_2/{sensor}/{crop}/{date}/individual_plants_out/"
        )
        for file in collection.data_objects:
            if re.search("volumes_entropy", file.name):
                _session.data_objects.get(
                    f"/iplant/home/shared/phytooracle/{season}/level_2/{sensor}/"
                    f"{crop}/{date}/individual_plants_out/{file.name}",
                    "local_file_delete.tar",
                    force=True,
                )
                break
        with tarfile.open("local_file_delete.tar", "r") as tar:
            tar.extractall()
        os.remove("local_file_delete.tar")


@st.cache_data
def download_plant_clustering_csv(_session, season, season_no):
    if not (os.path.exists(f"plant_clustering/season_{season_no}_clustering.csv")):
        if not os.path.exists("plant_clustering"):
            os.makedirs("plant_clustering")
        detection_combined = _session.collections.get(
            f"/iplant/home/shared/phytooracle/{season}/level_2/stereoTop/"
            f"season_{season_no}_plant_detection_combined"
        )
        for item in detection_combined.data_objects:
            _session.data_objects.get(
                f"/iplant/home/shared/phytooracle/{season}/level_2/stereoTop/"
                f"season_{season_no}_plant_detection_combined/"
                f"{item.name}",
                f"plant_clustering/season_{season_no}_clustering.csv",
                force=True,
            )
            break


@st.cache_resource
def combine_all_csv(path, sensor, crop, date):
    """Combine all the CSVs in a directory into a single pandas dataframe
    Args:
      - path (string):

    Returns:
      - dataframe: combined data
    """
    if not (os.path.exists(f"volumes_entropy/combined_csv_{sensor}-{crop}_{date}.csv")):
        if not os.path.exists("volumes_entropy"):
            os.makedirs("volumes_entropy")
        all_files = glob.glob(path + "/*.csv")
        li = []
        for filename in all_files:
            df = pd.read_csv(filename, index_col=None, header=0)
            li.append(df)

        result_frame = pd.concat(li, axis=0, ignore_index=True)
        result_frame.to_csv(
            f"volumes_entropy/combined_csv_{sensor}-{crop}_{date}.csv", index=False
        )
        print(os.getcwd())
        shutil.rmtree("3d_volumes_entropy_v009")

        return result_frame
    else:
        return pd.read_csv(f"volumes_entropy/combined_csv_{sensor}-{crop}_{date}.csv")


def create_filter(file_fetchers, combined_data, sensor):
    """Creates a dynamic fiter

    Args:
      - combined_data (pandas df): Everything in this dataframe
      - sensor (string): selected sensor
    """
    filter_options = []
    pn_exists = False
    extra = {
        "alwaysShowVerticalScroll": True,
        "alwaysShowHorizontalScroll": True,
        # "autoHeight": True,
    }
    
    for column_name in combined_data.columns:
        if column_name == "plant_name":
            pn_exists = True
        if not re.search(f"lon|lat|max|min|date", column_name, re.IGNORECASE):
            filter_options.append(column_name)

    if 'column_select' not in st.session_state:
        selected_column_name = filter_sec.selectbox(
            "Choose an Attribute", sorted(filter_options), key="column_select"
        )

    col1.header("All Data")
    all_gb = GridOptionsBuilder.from_dataframe(combined_data)

    # configure column definitions
    for column_name in combined_data.columns:
        all_gb.configure_column(column_name, filter=True)
    all_gb.configure_grid_options(**extra)
    gridOptions = all_gb.build()
    with col1:
        selected = AgGrid(
            combined_data,
            gridOptions=gridOptions,
            theme="balham",
            height=450,
            
        )

    selected_columns = []
    exact_column_name = selected_column_name
    for column_name in combined_data.columns:
        if re.search(
            f"{selected_column_name}|lon|lat|max|min|name|date",
            column_name,
            re.IGNORECASE,
        ):
            if re.search(selected_column_name, column_name, re.IGNORECASE):
                exact_column_name = column_name
            selected_columns.append(column_name)
    filtered_df = combined_data.loc[:, combined_data.columns.isin(selected_columns)]

    if selected_column_name == "genotype":
        selected_genotype = col2.selectbox("Genotype", filtered_df["genotype"].unique(), key="gf_date_one")
        filtered_df = filtered_df[filtered_df["genotype"].isin([selected_genotype])]

    if selected_column_name == "range":
        selected_range = col2.select_slider(
                        "Select a range: ", options=filtered_df["range"].unique()
                    )
        filtered_df = filtered_df[filtered_df["range"].isin([selected_range])]

    if selected_column_name == "plot":
        selected_plot = col2.select_slider(
                        "Select a plot: ", options=filtered_df["plot"].unique()
                    )
        filtered_df = filtered_df[filtered_df["plot"].isin([selected_plot])]


    col2.header("Filtered Data")
    filtered_gb = GridOptionsBuilder.from_dataframe(filtered_df)
    # configure column definitions
    for column_name in filtered_df.columns:
        filtered_gb.configure_column(column_name, filter=True)
    filtered_gb.configure_grid_options(**extra)
    filtered_gb.configure_selection(selection_mode="single", use_checkbox=True)
    gridOptions = filtered_gb.build()

    # set aggrid table to column two and watch for events
    with col2:
        selected = AgGrid(
            filtered_df,
            gridOptions=gridOptions,
            theme="balham",
            height=450,
           
        )  # get which row user selects of the

        # vizualization on point clouds is possible and a plant was selected use callback
        if selected["selected_rows"]:
            callback(file_fetchers, selected["selected_rows"][0]["plant_name"], selected["selected_rows"][0]["date"])

    col1.download_button(
        label="Download All Data",
        data=convert_df(combined_data),
        file_name=f"{combined_data.iloc[0, 0]}_combined_data.csv",
        mime="text/csv",
        
    )
    col2.download_button(
        label="Download Filtered Data (Co-ordinates + Selected Field)",
        data=convert_df(filtered_df),
        file_name=f"{combined_data.iloc[0, 0]}_filtered_data.csv",
        mime="text/csv",
        
    )

    
    get_visuals(file_fetchers, filtered_df, exact_column_name, pn_exists)


def callback(file_fetchers, crop_name, date):
    """
    Callback function to download 3D data of a plant, read a point cloud file, apply an offset to the x and y coordinates,
    create a DataFrame from the point cloud data, and generate a 3D scatter plot of the point cloud using Plotly.
    """
    date_key = date.split("__")
    file_fetcher = file_fetchers.get(date_key[0])
    # make change to accomodate filters bc table is not file
    plant_3d_data = file_fetcher.download_plant_by_index(crop_name)

    # return value is not correct path so get correct path
    path_final_file = f"individually_called_point_clouds/{crop_name}_timeseries/{file_fetcher.date}_final.ply"

    pcd = o3d.io.read_point_cloud(path_final_file)
    # Apply offset after opening the point cloud
    x_offset = 409000
    y_offset = 3660000

    #   adjust based on offsets
    points = np.asarray(pcd.points)
    xs = points[:, 0] - x_offset
    ys = points[:, 1] - y_offset
    zs = points[:, 2]
    df_dict = {"x": xs, "y": ys, "z": zs}
    df = pd.DataFrame(df_dict)
    # Use plotly to display stuff
    color_scale = [[0.0, "yellow"], [1.0, "green"]]
    fig = px.scatter_3d(
        df,
        title=crop_name,
        x="x",
        y="y",
        z="z",
        color="z",
        color_continuous_scale=color_scale,
    )

    fig.update_traces(marker=dict(size=3))
    dist_col.plotly_chart(fig, use_container_width=True)


def get_visuals(filtered_df, column_name, pn_exists):
    """Make the map plot, as well as the histogram based on the selected filed

    Args:
      - filtered_df (dataframe): Pandas df that has Co-ordinates +  selected field
      - column_name (dataframe): Selected column
    """
    # Emmanuel's API key, Might need to change this
    px.set_mapbox_access_token(
        "pk.eyJ1IjoiZW1tYW51ZWxnb256YWxleiIsImEiOiJja3RndzZ2NmIwbTJsMnBydGN1NWJ4bzkxIn0.rtptqiaoqpDIoXsw6Qa9lg"
    )
    if pn_exists:
        fig = px.scatter_mapbox(
            filtered_df,
            lat="lat",
            lon="lon",
            color=column_name,
            zoom=16.6,
            opacity=1,
            mapbox_style="satellite-streets",
            hover_data=["lat", "lon", column_name, "plant_name"],
        )
    else:
        fig = px.scatter_mapbox(
            filtered_df,
            lat="lat",
            lon="lon",
            color=column_name,
            zoom=16.6,
            opacity=1,
            mapbox_style="satellite-streets",
            hover_data=["lat", "lon", column_name],
        )

    # Change color scheme
    fig.update_traces(marker=dict(colorscale="Viridis"))

    # Change layout
    fig.update_layout(
        title="Plotly Map",
        geo_scope="usa",
        autosize=True,
        font=dict(family="Courier New, monospace", size=18, color="RebeccaPurple"),
    )

    plotly_col.plotly_chart(fig, use_container_width=True)

def handle_except(_session, seasons, selected_season, dates, selected_date, selected_sensor, selected_crop, alt_layout):
    
    plant_detection_csv_path = get_plant_detection_csv_path(
        _session,
        seasons[selected_season],
        selected_sensor,
        selected_crop,
        dates,
        selected_date,
        alt_layout,
    )
    if plant_detection_csv_path != "":
        # Download necessary files (just fieldbook and plantdetection csv for now)
        print(plant_detection_csv_path)
        with filter_sec:
            with st.spinner(
                "This might take some time. Please wait..."
            ):
                field_book_name = download_fieldbook(
                    _session, seasons[selected_season]
                )
                if field_book_name == "":
                    st.write(
                        "No fieldbook for this season was found"
                    )
                else:
                    local_file_name = (
                        plant_detection_csv_path.split("/")[
                            -1
                        ].split(".")[0]
                    )
                    local_file_name = f"{local_file_name[: len(local_file_name) - 4]}ion.csv"
                    local_file_name = (
                        f"{dates[selected_date]}_fluorescence_aggregation"
                        if selected_sensor == "ps2Top"
                        else local_file_name
                    )

                    download_plant_detection_csv(
                        _session,
                        local_file_name,
                        plant_detection_csv_path,
                    )
                    plant_detect_df = (
                        pd.read_csv(f"detect_out/{local_file_name}")
                        if selected_sensor != "ps2Top"
                        else pd.read_csv(
                            f"fluorescence_aggregation_out/"
                            f"{dates[selected_date]}_fluorescence_aggregation.csv"
                        )
                    )
                    # Data Analysis and vis section starts
                    file_fetcher, result = data_analysis(
                                                _session,
                                                seasons[selected_season],
                                                plant_detect_df,
                                                field_book_name,
                                                selected_sensor,
                                                selected_crop,
                                                dates[selected_date],
                                                alt_layout,
                                            )
                    return file_fetcher, result



                    
def main():
    # Setting up the app for aesthetic changes
    st.set_page_config(
        page_title="Phytooracle Dashboard", page_icon=":seedling:", layout="wide"
    )
    hide_default_format = """
      <style>
      .reportview-container.main.block-container {{
      padding-top: 0rem;
      }}
      .block-container {
        padding-top: 2rem;
        padding-bottom: 0rem;
      }
      footer {visibility: hidden;}
      </style>
      """
    st.markdown(hide_default_format, unsafe_allow_html=True)
    st.sidebar.title(":green[Phytooracle] :seedling:")
    st.sidebar.subheader("Scan selection")
    st.title("Dashboard")
    
    # To establish an irods _session
    try:
        _session = iRODSSession(
            host="data.cyverse.org",
            port=1247,
            user="phytooracle",
            password="mac_scanalyzer",
            zone="iplant",
        )
    except:
        st.write("Something went wrong establishing a iRODS session. Contact support.")
    else:
        seasons = get_seasons(_session)
        if seasons:
            season_display_names = sorted(
                seasons.keys(), key=lambda x: int(x.split(" ")[1])
            )
            selected_season = st.sidebar.selectbox(
                "Select a season: ", season_display_names
            )
            # A flag to check wheter S10 or S11 is selected (Exceptions)
            alt_layout = (
                True
                if (selected_season == "Season 10" or selected_season == "Season 11")
                else False
            )
            sensors = get_sensors(_session, seasons[selected_season])
            if sensors:
                selected_sensor = st.sidebar.selectbox("Select a sensor: ", sensors)
                crops = get_crops(
                    _session, seasons[selected_season], selected_sensor, alt_layout
                )
                if alt_layout or crops:
                    selected_crop = ""
                    if crops:
                        selected_crop = st.sidebar.selectbox(
                            "Select a crop: ", sorted(crops)
                        )
                display_processing_info(
                    _session, seasons, selected_season, sensors, selected_crop
                )
                dates = get_dates(
                    _session, seasons[selected_season], selected_sensor, selected_crop
                )
                # for organizing stuff on the screen
                global col1, col2, filter_sec, vis_container, plotly_col, dist_col
                file_fetchers ={}
                filter_sec = st.container()
                vis_container = st.container()
                plotly_col, dist_col = vis_container.columns(2)
                col1, col2 = st.columns(2)
                extra_selected_dates = []
                if dates:
                    display_dates = sorted(dates.keys())
                    selected_date = st.sidebar.select_slider(
                        "Select a date: ", options=display_dates
                    )

                    num_opts = list(np.arange(len(dates)))
                    extra_date = st.sidebar.checkbox("Select additional dates")
                    selected_extra_dates = []
                    if extra_date:
                        num_extra_dates=st.sidebar.selectbox("Number of Additional dates", num_opts, key="num_opts")
                        for i in range(num_extra_dates):
                            print(i)
                            extra_selected_date = st.sidebar.select_slider(
                                "Select another date: ", options=display_dates, key=f"extra_date_{i}"
                            )
                    

                    filter_sec.header(":blue[Data and its Visualization]")
                    #TRY FOR FIRST DATE
                    try:
                        _session.data_objects.get(
                            f"/iplant/home/shared/phytooracle/dashboard_cache/{selected_sensor}/combined_data/"
                            f"{seasons[selected_season]}_{dates[selected_date]}_all.csv",
                            f"{seasons[selected_season]}_{dates[selected_date]}_all.csv",
                            force=True,
                        )
                        comb_df = pd.read_csv(
                            f"{seasons[selected_season]}_{dates[selected_date]}_all.csv"
                        )
                        ff = create_file_fetcher(
                            _session,
                            seasons[selected_season],
                            dates[selected_date],
                            selected_crop,
                        )

                        file_fetchers[selected_date]=ff
                    except Exception as e:
                        print(e)
                        ff, comb_df = handle_except(_session, seasons, selected_season, dates, selected_date, selected_sensor, selected_crop, alt_layout)

                        file_fetchers[selected_date]=ff
                   
                    if not extra_date:
        
                        if comb_df is not None:
                            create_filter(file_fetchers, comb_df, selected_sensor)
                        if os.path.exists(f"{seasons[selected_season]}_{dates[selected_date]}_all.csv"):
                            os.remove(
                                f"{seasons[selected_season]}_{dates[selected_date]}_all.csv"
                            )
                    else:

                        new_df = pd.DataFrame()
                        new_df = pd.concat([new_df, comb_df])
                        for i in range(num_extra_dates):

                            esd = st.session_state[f"extra_date_{i}"]
                            print(esd)
                            print("\n\n")
                            if esd != selected_date:
                                try:
                                    _session.data_objects.get(
                                        f"/iplant/home/shared/phytooracle/dashboard_cache/{selected_sensor}/combined_data/"
                                        f"{seasons[selected_season]}_{dates[esd]}_all.csv",
                                        f"{seasons[selected_season]}_{dates[esd]}_all.csv",
                                        force=True,
                                    )
                                    comb_df_add = pd.read_csv(
                                        f"{seasons[selected_season]}_{dates[esd]}_all.csv"
                                    )
                                    ff_additional = create_file_fetcher(
                                        _session,
                                        seasons[selected_season],
                                        dates[esd],
                                        selected_crop,
                                    )

                                    file_fetchers[esd]=ff_additional
                                except Exception as e:
                                    print(e)
                                    ff_additional, comb_df_add = handle_except(_session, seasons, selected_season, dates, esd, selected_sensor, selected_crop, alt_layout)

                                new_df = pd.concat([new_df, comb_df_add])
                                if os.path.exists(f"{seasons[selected_season]}_{dates[esd]}_all.csv"):
                                    os.remove(
                                        f"{seasons[selected_season]}_{dates[esd]}_all.csv"
                                    )
                            
                        if new_df is not None:
                                create_filter(file_fetchers, new_df, selected_sensor)

                        if os.path.exists(f"{seasons[selected_season]}_{dates[selected_date]}_all.csv"):
                                os.remove(
                                    f"{seasons[selected_season]}_{dates[selected_date]}_all.csv"
                                )
                   
if __name__ == "__main__":
    main()
