from webdav3.client import Client
from datetime import datetime, timedelta
from st_aggrid import AgGrid, GridOptionsBuilder
import pandas as pd
import streamlit as st
import plotly.express as px
import requests
import re
import os
import tarfile
import glob
import shutil  # remove filled directory to manage space
import json
import fetch_ipc as fipc
import streamlit.components.v1 as components
import numpy as np


@st.cache_data
def get_date_list(
    _session, index, selected_season, selected_sensor, selected_crop, level
):
    try:
        path = index[selected_season.split(" ")[1]]["paths"][level][selected_crop][
            selected_sensor
        ]
    except:
        st.write(f"Path missing: {path}")
        return {}
    else:
        try:
            subfolderlist = _session.list(path)
            date_pattern = r"\d{4}-\d{2}-\d{2}"
            date_substrings = {}
            for string in subfolderlist:
                date = re.search(date_pattern, string)
                if date:
                    if (
                        len(index[selected_season.split(" ")[1]]["metadata"]["crops"])
                        > 1
                    ):
                        if re.search(selected_crop, string):
                            date_substrings[date.group()] = string
                    else:
                        date_substrings[date.group()] = string
            return date_substrings
        except Exception as e:
            st.write("No data for this sensor and crop in this season. ")
            return {}


@st.cache_data
def display_processing_info(_session, index, sensors, season, crop):
    info_sec = st.container()
    info_sec.divider()
    info_sec.header(":blue[Processing Information]")
    info_sec.markdown(
        f"Here is the status of processing for different sensor data for :red[{season}] **({crop})**"
    )
    sensor_df = pd.DataFrame(
        {"sensor": [], "level": [], "processed": [], "unprocessed": []}
    )
    all_avbl_lvls = sorted(
        list(index[season.split(" ")[1]]["paths"].keys()), reverse=True
    )
    processed_files_ct_all = 0
    total_files_ct_all = 0
    for level in all_avbl_lvls[0:-1]:
        level_items = index[season.split(" ")[1]]["paths"][level]
        if crop in level_items.keys():
            for sensor in list(level_items[crop].keys()):
                if sensor not in sensor_df["sensor"].tolist():
                    level_0_cnt = len(
                        get_date_list(_session, index, season, sensor, crop, "0")
                    )
                    total_files_ct_all += level_0_cnt
                    # the sensor data for the level is a composite file
                    if "." in level_items[crop][sensor]:
                        processed_files_ct_all += level_0_cnt
                        new_row = pd.DataFrame(
                            {
                                "sensor": [sensor],
                                "level": [level],
                                "processed": [level_0_cnt],
                                "unprocessed": [0],
                            }
                        )
                    else:
                        level_X_cnt = len(
                            get_date_list(_session, index, season, sensor, crop, level)
                        )
                        processed_files_ct_all += level_X_cnt
                        new_row = pd.DataFrame(
                            {
                                "sensor": [sensor],
                                "level": [level],
                                "processed": [level_X_cnt],
                                "unprocessed": [level_0_cnt - level_X_cnt],
                            }
                        )
                    sensor_df = pd.concat([sensor_df, new_row], ignore_index=True)
    processing_summary = ""
    for index, row in sensor_df.iterrows():
        percent_processed = (
            row["processed"] / (row["processed"] + row["unprocessed"])
        ) * 100
        processing_summary += f"**:green[{row['sensor'].upper()}]** processed to **Level {row['level']}** (:orange[{round(percent_processed, 2)}%]), "
    info_sec.markdown(processing_summary[:-2])
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
                text=f"{total_files_ct_all - processed_files_ct_all} file(s) left",
                x=0.5,
                y=0.5,
                font_size=30,
                showarrow=False,
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


@st.cache_data
def download_file(remote_path, local_folder, local_name):
    options = {
        "webdav_hostname": "https://data.cyverse.org/dav",
        "webdav_login": "phytooracle",
        "webdav_password": "mac_scanalyzer",
        "webdav_root": "/",
    }
    if not os.path.exists(local_folder):
        os.makedirs(local_folder)
    try:
        # using requests module as webdav3 causing problems while downloading files
        response = requests.get(
            f'{options["webdav_hostname"]}{remote_path}',
            auth=(options["webdav_login"], options["webdav_password"]),
        )
        file_extn = remote_path.split(".")[1]
        if response.status_code == 200:
            # Save the content to a local file
            with open(f"{local_folder}/{local_name}.{file_extn}", "wb") as local_file:
                local_file.write(response.content)
            if file_extn == "tar" or file_extn == "gz":
                with tarfile.open(
                    f"{local_folder}/{local_name}.{file_extn}", "r"
                ) as tar:
                    tar.extractall(path=local_folder)
                    extracted_items = tar.getnames()
                    os.remove(f"{local_folder}/{local_name}.{file_extn}")
                    return f"{local_folder}/{extracted_items[1]}"
            return f"{local_folder}/{local_name}.{file_extn}"
    except Exception as e:
        print(e)
        return ""


@st.cache_data
def get_plant_detection_csv(_session, index, season, sensor, crop, date):
    if re.search("3d", sensor, re.IGNORECASE):
        # 3D sensors don't have Plant Detection CSVs, so we find approx RGB date
        dates_RGB = get_date_list(_session, index, season, "stereoTop", crop, "0")
        date_RGB = ""
        date_3D_obj = datetime.strptime(date, "%Y-%m-%d")
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
                f"No Plant Detection CSV is present for this sensor on this date. ({date})"
            )
            return ""
        date = date_RGB
        sensor = "stereoTop"
    plant_detection_folder_p = index[season.split(" ")[1]]["metadata"]["plant-detect"][
        crop
    ][sensor]
    # sublist = _session.list(plant_detection_folder_p, get_info=True)
    sublist = _session.list(plant_detection_folder_p)
    date_pattern = r"\d{4}-\d{2}-\d{2}"
    for name in sublist:
        potential_date = re.search(date_pattern, name)
        if potential_date:
            if potential_date.group() == date:
                path = plant_detection_folder_p + "/" + name
                path_set = False
                # is a dir.
                if "/" in name:
                    item_list = _session.list(path)
                    for item in item_list:
                        if sensor != "ps2Top":
                            if "detect_out" in item:
                                path = f"{path}{item}"
                                path_set = True
                                break
                        else:
                            if "aggregation_out" in item:
                                path = f"{path}{item}"
                                path_set = True
                                break
                else:
                    # not a dir
                    path_set = True
                if path_set:
                    try:
                        local_file = download_file(
                            path,
                            "plant_detection",
                            f"{crop}_{sensor}_{date}_{season.split(' ')[1]}",
                        )
                        print("Plant Detection CSV downloaded")
                        return local_file
                    except Exception as e:
                        print(e)
                        return ""
    return ""


@st.cache_data
def download_fieldbook(index, season):
    try:
        file_loc = index[season.split(" ")[1]]["metadata"]["fieldbook"]
        local_file = download_file(
            file_loc, "field_books", f"season_{season.split(' ')[1]}_fieldbook"
        )
        print(f"Fieldbook downloaded.")
        return local_file
    except Exception as e:
        print(e)
        return ""


def data_analysis(
    _session,
    index,
    season,
    crop,
    sensor,
    date,
    field_book_name,
    plant_detect_name,
):
    # make field book dataframe based on its extension
    if field_book_name.split(".")[1] == "xlsx":
        try:
            field_book_df = pd.read_excel(
                io=field_book_name,
                sheet_name=lambda sheet: "field" in sheet.lower()
                and "book" in sheet.lower(),
                engine="openpyxl",
            )
        except Exception as e:
            print(e)
            return
    elif field_book_name.split(".")[1] == "csv":
        field_book_df = pd.read_csv(field_book_name)
    else:
        st.write(
            f"Can't deal with files with the extension {field_book_name.split('.')[1]}."
        )
        st.write("Please contact the Phytooracle staff")
        return
    plant_detect_df = pd.read_csv(plant_detect_name)
    plant_detect_df = plant_detect_df.rename(columns={"Plot": "plot"})
    try:
        result = plant_detect_df.merge(field_book_df, on="plot")
        update = extra_files(_session, result, index, season, crop, sensor, date)
        if not re.search("ps2", sensor, re.IGNORECASE) and not update.empty:
            result = update
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
            st.dataframe(result)
            # file_fetcher = create_file_fetcher(_session, season, date, crop)
            # create_filter(file_fetcher, combined_data=result, sensor=sensor)

        else:
            if not update.empty:
                result = update
            st.subheader(f"Visualizations are not available for {sensor} sensor")
            st.write("Combined Data is available for download")
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
            st.download_button(
                label="Download Plant Detection CSV (Separately)",
                data=convert_df(plant_detect_df),
                file_name=f"{date}_plant_detect_out.csv",
                mime="text/csv",
            )
            st.download_button(
                label="Download Fieldbook Data (Separately)",
                data=convert_df(field_book_df),
                file_name=f"season_{season.split(' ')[1]}_fieldbook.csv",
                mime="text/csv",
            )
            st.download_button(
                label="Download Combined Data (FieldBook + Plant Detection)",
                data=convert_df(result),
                file_name=f"{date}_combined_data.csv",
                mime="text/csv",
            )
    except Exception as e:
        # result dataframe is empty or has not been created, allow the users to download the plant detection csv and fieldbook csv
        print(e)
        st.subheader("Some Error occured (might be a merge issue).")
        st.write(
            "Below are the links to download the Plant Detection and the Fieldbook CSV"
        )
        st.download_button(
            label="Download Plant Detection CSV",
            data=convert_df(plant_detect_df),
            file_name=f"{date}_plant_detect_out.csv",
            mime="text/csv",
        )
        st.download_button(
            label="Download Fieldbook Data",
            data=convert_df(field_book_df),
            file_name=f"{season.split(' ')[1]}_fieldbook.csv",
            mime="text/csv",
        )


def extra_files(_session, combined_df, index, season, crop, sensor, date):
    if "3D" in sensor:
        download_loc = dload_indv_plant_data_3D(_session, index, season, crop, date)
        if download_loc != "":
            plant_data_path = combine_csv_into_one(
                download_loc, f"{crop}_{date}_{season.split(' ')[1]}"
            )
            plant_clustering_path = download_plant_clustering_csv(
                index, season, "stereoTop"
            )
            if download_loc != "":
                plant_clustering_df = pd.read_csv(download_loc).loc[
                    :, ["plant_name", "lat", "lon"]
                ]
                ind_plant_df = pd.read_csv(plant_data_path)
                # Taking very long time - 3 mins (try merging plant_clustering with ind first)
                ind_plant_df = ind_plant_df.merge(plant_clustering_df, on="plant_name")
                combined_df = combined_df.merge(ind_plant_df, on=["lat", "lon"])
                return combined_df
        return pd.DataFrame()
    else:
        if re.search("ps2", sensor, re.IGNORECASE):
            path = download_plant_clustering_csv(index, season, "stereoTop")
            if path != "":
                try:
                    plant_clustering_df = pd.read_csv(path).loc[
                        :, ["plant_name", "plot", "genotype", "lat", "lon"]
                    ]
                    combined_df = combined_df.merge(
                        plant_clustering_df, on=["plot", "genotype"]
                    )
                    return combined_df
                except:
                    return pd.DataFrame()
            else:
                path = download_plant_clustering_csv(index, season, sensor)
                if path != "":
                    try:
                        plant_clustering_df = pd.read_csv(path).loc[
                            :, ["plant_name", "lat", "lon"]
                        ]
                        combined_df = combined_df.merge(
                            plant_clustering_df, on=["lat", "lon"]
                        )
                        return combined_df
                    except:
                        return pd.DataFrame()
    return pd.DataFrame()


def dload_indv_plant_data_3D(_session, index, season, crop, date):
    try:
        path = index[season.split(" ")[1]]["metadata"]["volume-entropy"][crop]
        sublist = _session.list(path)
        date_found = False
        for file in sublist:
            if date in file:
                path = f"{path}/{file}/individual_plants_out"
                date_found = True
                break
        if date_found:
            sublist = _session.list(path)
            for file in sublist:
                if re.search("volumes_entropy", file, re.IGNORECASE):
                    path = download_file(
                        f"{path}/{file}",
                        "indv_plant_data",
                        f"{crop}_{date}_{season.split(' ')[1]}",
                    )
                    path = path.split("/")[0:-1]
                    path = "/".join(path)
                    return path
        st.write(f"Couldn't find the necessary file for plant vis. for this date")
        return ""
    except Exception as e:
        print(e)
        st.write(
            f"Problem occurered while downloading necessary files for 3D sensor. Contact Phytooracle Staff"
        )
        return ""


def combine_csv_into_one(folder, csv_name):
    try:
        all_files = glob.glob(folder + "/*.csv")
        li = []
        for filename in all_files:
            # to avoid other date csv
            pattern = r"\w+_\d{4}-\d{2}-\d{2}_\d{2}"
            if not re.match(pattern, filename):
                df = pd.read_csv(filename, index_col=None, header=0)
                li.append(df)
        result_frame = pd.concat(li, axis=0, ignore_index=True)
        result_frame.to_csv(f"indv_plant_data/{csv_name}.csv", index=False)
        shutil.rmtree(folder)
        return f"indv_plant_data/{csv_name}.csv"
    except:
        return ""


def download_plant_clustering_csv(index, season, sensor):
    try:
        path = index[season.split(" ")[1]]["metadata"]["plant-clustering"][sensor]
        return download_file(
            path, "plant_clustering", f"season_{season.split(' ')[1]}_{sensor}_pc"
        )
    except Exception as e:
        print(e)
        return ""


@st.cache_data
def convert_df(df):
    # Cache the conversion to prevent computation on every rerun
    return df.to_csv(index=False).encode("utf-8")


# def create_file_fetcher(_session, season, date, crop):


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
    # To access Cyverse WebDAV
    try:
        options = {
            "webdav_hostname": "https://data.cyverse.org/dav",
            "webdav_login": "phytooracle",
            "webdav_password": "mac_scanalyzer",
            "webdav_root": "/",
        }
        _session = Client(options)
    except:
        st.write("Something went wrong establishing a iRODS session. Contact support.")
    else:
        with open("index.json", "r") as file:
            index = json.load(file)
        avbl_seasons = [
            f"Season {item}" for item in sorted(index.keys(), key=lambda x: int(x))
        ]
        selected_season = st.sidebar.selectbox("Select a season: ", avbl_seasons)
        avbl_sensors = index[selected_season.split(" ")[1]]["metadata"]["sensors"]
        selected_sensor = st.sidebar.selectbox("Select a sensor: ", avbl_sensors)
        avbl_crops = index[selected_season.split(" ")[1]]["metadata"]["crops"]
        selected_crop = st.sidebar.selectbox("Select a crop: ", avbl_crops)
        level_0_dates = get_date_list(
            _session, index, selected_season, selected_sensor, selected_crop, "0"
        )
        display_processing_info(
            _session, index, avbl_sensors, selected_season, selected_crop
        )
        if len(level_0_dates) != 0:
            selected_date = st.sidebar.select_slider(
                "Select a date:", list(level_0_dates.keys())
            )
            date_two = st.sidebar.checkbox("Select another date")
            if date_two:
                selected_date_two = st.sidebar.select_slider(
                    "Select a second date: ", list(level_0_dates.keys())
                )
            # for organizing stuff on the screen
            global col1, col2, filter_sec, vis_container, plotly_col, dist_col
            filter_sec = st.container()
            vis_container = st.container()
            plotly_col, dist_col = vis_container.columns(2)
            col1, col2 = st.columns(2)
            filter_sec.header(":blue[Data and its Visualization]")
            try:
                # implement cache
                a = 1 / 0
                pass
            except:
                plant_detect_name = get_plant_detection_csv(
                    _session,
                    index,
                    selected_season,
                    selected_sensor,
                    selected_crop,
                    selected_date,
                )
                if plant_detect_name == "":
                    st.write(
                        f"No Plant Detection CSV is present for this sensor on this date."
                    )
                else:
                    with filter_sec:
                        with st.spinner("This might take some time. Please wait..."):
                            field_book_name = download_fieldbook(index, selected_season)
                            if field_book_name == "":
                                st.write("No fieldbook found for this season")
                            else:
                                data_analysis(
                                    _session,
                                    index,
                                    selected_season,
                                    selected_crop,
                                    selected_sensor,
                                    selected_date,
                                    field_book_name,
                                    plant_detect_name,
                                )
                                print("Good Job")


if __name__ == "__main__":
    main()
