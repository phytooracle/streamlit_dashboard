from webdav3.client import Client
from datetime import datetime, timedelta
from st_aggrid import AgGrid, GridOptionsBuilder
import pandas as pd
import streamlit as st
import plotly.express as px
import open3d as o3d
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
import traceback


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
        # specifically done this for the point clouds index
        try:
            file_extn = remote_path.split(".")[1]
        except:
            file_extn = "txt"
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
        print(traceback.format_exc())
        return ""


@st.cache_data
def get_plant_detection_csv(_session, index, season, sensor, crop, date):
    if re.search("3d", sensor, re.IGNORECASE):
        # 3D sensors don't have Plant Detection CSVs, so we find approx RGB date
        date_RGB = get_closest_date(
            _session, index, season, "stereoTop", crop, "0", date
        )[0]
        if not date_RGB:
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
                        print(traceback.format_exc())
                        return ""
    return ""


def get_closest_date(_session, index, season, sensor, crop, level, date):
    dates = get_date_list(_session, index, season, sensor, crop, level)
    closest_date = []
    date_3D_obj = datetime.strptime(date, "%Y-%m-%d")
    # to check neighboring dates in case of no exact match
    for date_id in dates.keys():
        dte_obj = datetime.strptime(date_id, "%Y-%m-%d")
        if (date_3D_obj == dte_obj) or (
            date_3D_obj == dte_obj + timedelta(days=2)
            or date_3D_obj == dte_obj - timedelta(days=2)
        ):
            closest_date = [date_id, dates[date_id]]
        if (date_3D_obj == dte_obj):
            break
    print(closest_date)
    return closest_date


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
        print(traceback.format_exc())
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
            all_sheets = pd.read_excel(
                io=field_book_name,
                sheet_name=None,
                engine="openpyxl",
            )

            # Filter sheets based on condition
            filtered_sheets = [
                sheet
                for sheet in all_sheets.keys()
                if "field" in sheet.lower() and "book" in sheet.lower()
            ]

            # Read the desired sheet
            field_book_df = pd.read_excel(
                io=field_book_name,
                sheet_name=filtered_sheets[
                    0
                ],  # Use the first sheet that satisfies the condition
                engine="openpyxl",
            )

        except Exception as e:
            print(traceback.format_exc())
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
    field_book_df = field_book_df.rename(columns={"Plot": "plot"})
    # to fix season 10 and 11
    if season == "Season 10":

        if sensor == "stereoTop" or sensor == "scanner3DTop":
            plant_detect_df["plot"] = plant_detect_df["plot"].str.split(
                "_", expand=True
            )[6].str.zfill(2) + plant_detect_df["plot"].str.split("_", expand=True)[
                8
            ].str.zfill(
                2
            )
            plant_detect_df["plot"] = plant_detect_df["plot"].astype(int)
        elif sensor == "flirIrCamera":
            plant_detect_df["plot"] = plant_detect_df["plot"].str.split(
                "_", expand=True
            )[6].str.zfill(2) + plant_detect_df["plot"].str.split("_", expand=True)[
                8
            ].str.zfill(
                2
            )
        elif sensor == "ps2Top":
            plant_detect_df["plot"] =plant_detect_df["Plot"].str.split(
                " ", expand=True
            )[6].str.zfill(2) + plant_detect_df["Plot"].str.split(" ", expand=True)[
                8
            ].str.zfill(
                2
            )
    if season == " Season 11":
        plant_detect_df["plot"] = plant_detect_df["plot"].astype(str).str.zfill(4)
    try:
        plant_detect_df["plot"] = plant_detect_df["plot"].astype(int)
        field_book_df["plot"] = field_book_df["plot"].astype(int)
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
            file_fetcher = create_file_fetcher(_session, index, season, date, crop)
            # maybe add try/except here
            create_filter(file_fetcher, result)
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
                label="Download Combined Data",
                data=convert_df(result),
                file_name=f"{date}_combined_data.csv",
                mime="text/csv",
            )
    except Exception as e:
        # result dataframe is empty or has not been created, allow the users to download the plant detection csv and fieldbook csv
        print(traceback.format_exc())
        st.subheader("Some Error occured (might be a merge issue).")
        st.dataframe(field_book_df)
        st.dataframe(plant_detect_df)
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
    combined_df[["lat", "lon"]] = (
        combined_df[["lat", "lon"]].apply(pd.to_numeric).apply(lambda x: round(x, 7))
    )
    if "3D" in sensor:
        download_loc = dload_indv_plant_data_3D(_session, index, season, crop, date)
        if download_loc != "":
            plant_data_path = combine_csv_into_one(
                download_loc, f"{crop}_{date}_{season.split(' ')[1]}"
            )
            plant_clustering_path = download_plant_clustering_csv(
                index, season, "stereoTop"
            )
            if plant_clustering_path != "":
                plant_clustering_df = pd.read_csv(plant_clustering_path).loc[
                    :, ["plant_name", "lat", "lon"]
                ]
                plant_clustering_df[["lat", "lon"]] = (
                    plant_clustering_df[["lat", "lon"]]
                    .apply(pd.to_numeric)
                    .apply(lambda x: round(x, 7))
                )
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
                    plant_clustering_df[["lat", "lon"]] = (
                        plant_clustering_df[["lat", "lon"]]
                        .apply(pd.to_numeric)
                        .apply(lambda x: round(x, 7))
                    )
                    combined_df = combined_df.merge(
                        plant_clustering_df, on=["plot", "genotype"]
                    )
                    return combined_df
                except Exception as e:
                    print(traceback.format_exc())
                    return pd.DataFrame()
        else:
            path = download_plant_clustering_csv(index, season, sensor)
            if path != "":
                try:
                    plant_clustering_df = pd.read_csv(path).loc[
                        :,
                        ["plant_name", "lat", "lon"],
                    ]
                    plant_clustering_df[["lat", "lon"]] = (
                        plant_clustering_df[["lat", "lon"]]
                        .apply(pd.to_numeric)
                        .apply(lambda x: round(x, 7))
                    )
                    combined_df = combined_df.merge(
                        plant_clustering_df, on=["lat", "lon"]
                    )
                    return combined_df
                except:
                    print(traceback.format_exc())
                    return pd.DataFrame()
    return pd.DataFrame()


def dload_indv_plant_data_3D(_session, index, season, crop, date):
    try:
        path = index[season.split(" ")[1]]["metadata"]["volume-entropy"][crop]
        print(path)
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
        print(traceback.format_exc())
        st.write(
            f"Problem occurered while downloading necessary files for 3D sensor. Contact Phytooracle Staff"
        )
        return ""


def combine_csv_into_one(folder, csv_name):
    if os.path.exists(f"indv_plant_data/{csv_name}.csv"):
        return f"indv_plant_data/{csv_name}.csv"
    try:
        all_files = glob.glob(folder + "/*.csv")
        li = []
        for filename in all_files:
            # to avoid other date csv
            pattern = r"\w+_\d{4}-\d{2}-\d{2}_\d{2}.csv"
            if not re.match(pattern, filename):
                df = pd.read_csv(filename, index_col=None, header=0)
                li.append(df)
        if li != []:
            result_frame = pd.concat(li, axis=0, ignore_index=True)
            result_frame.to_csv(f"indv_plant_data/{csv_name}.csv", index=False)
            shutil.rmtree(folder)
            return f"indv_plant_data/{csv_name}.csv"
        return ""
    except:
        print(traceback.format_exc())
        return ""


def download_plant_clustering_csv(index, season, sensor):
    if os.path.exists(
        f"plant_clustering/season_{season.split(' ')[1]}_{sensor}_pc.csv"
    ):
        return f"plant_clustering/season_{season.split(' ')[1]}_{sensor}_pc.csv"
    try:
        path = index[season.split(" ")[1]]["metadata"]["plant-clustering"][sensor]
        return download_file(
            path, "plant_clustering", f"season_{season.split(' ')[1]}_{sensor}_pc"
        )
    except Exception as e:
        print(traceback.format_exc())
        return ""


@st.cache_data
def convert_df(df):
    # Cache the conversion to prevent computation on every rerun
    return df.to_csv(index=False).encode("utf-8")


def create_file_fetcher(_session, index, season, date, crop):
    closest_date = get_closest_date(
        _session, index, season, "scanner3DTop", crop, "2", date
    )[1]
    if closest_date is None:
        # There are no level 2 point clouds for the chosen date or any date near it
        file_fetcher = None
    else:
        try:
            level_2_path = index[season.split(" ")[1]]["paths"]["2"][crop]
            cyverse_path = f"{level_2_path}/{closest_date}/individual_plants_out/{closest_date}_segmentation_pointclouds_index"
            local_idx_path = download_file(
                cyverse_path,
                "visualization",
                f"{closest_date}_segmentation_pointclouds_index",
            )
            file_fetcher = fipc.Fetcher(
                "individually_called_point_clouds",
                season,
                "level_2",
                closest_date,
                crop,
                local_idx_path,
            )
        except:
            print(traceback.format_exc())
            return None
    return file_fetcher


def create_filter(file_fetcher, combined_data):
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
        "autoHeight": True,
    }

    for column_name in combined_data.columns:
        if column_name == "plant_name":
            pn_exists = True
        if not re.search(f"lon|lat|max|min|date", column_name, re.IGNORECASE):
            filter_options.append(column_name)
    selected_column_name = filter_sec.selectbox(
        "Choose an Attribute", sorted(filter_options)
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
        selected_genotype = col2.selectbox(
            "Genotype", filtered_df["genotype"].unique(), key="gf_date_one"
        )

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
            callback(file_fetcher, selected["selected_rows"][0]["plant_name"])

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
    get_visuals(file_fetcher, filtered_df, exact_column_name, pn_exists)


def get_visuals(file_fetcher, filtered_df, column_name, pn_exists):
    """Make the map plot, as well as the histogram based on the selected filed

    Args:
      - filtered_df (dataframe): Pandas df that has Co-ordinates +  selected field
      - column_name (dataframe): Selected column
    """
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


def callback(file_fetcher, crop_name):
    """
    Callback function to download 3D data of a plant, read a point cloud file, apply an offset to the x and y coordinates,
    create a DataFrame from the point cloud data, and generate a 3D scatter plot of the point cloud using Plotly.
    """
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
