"""
Author: Aditya K. 
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


@st.cache_data
def convert_df(df):
    # Cache the conversion to prevent computation on every rerun
    return df.to_csv(index=False).encode("utf-8")


def get_seasons(session):
    """A method to download the list of seasons and update them through Cyverse.

    Args:
        session (irodsSession): A session object that allows the program to query data
        from the Cyverse datastore.

    Returns:
        seasons (dict) : A dictionary that looks like this ->
        {'The season name to be displayed': 'Actual name we got'}
    """
    seasons = {}
    try:
        root_collection = session.collections.get("/iplant/home/shared/phytooracle")
    except:
        st.write("No data present on Cyverse datastore for any season. ")
        return seasons
    else:
        for directory in root_collection.subcollections:
            if re.search("season*", directory.name):
                seasons["Season " + directory.name.split("_")[1]] = directory.name
        return seasons


def get_sensors(session, season):
    """A method to download the sensors available for a given season
    and update them through Cyverse.

    Args:
        session (irodsSession): A session object that allows the program to query data
        from the Cyverse datastore.
        season (string): The actual name that appears on cyverse for the selected season

    Returns:
        sensors (list): A list of the available sensors.
    """
    sensors = []
    try:
        sensor_collection = session.collections.get(
            f"/iplant/home/shared/phytooracle/{season}/level_1"
        )
    except:
        st.write("None of the sensor data has been processed yet for this season. ")
        return sensors
    else:
        for sensor in sensor_collection.subcollections:
            # might need to modify this for extra sensors - ASK ABOUT DRONE DATA
            if not re.search("depreceated", sensor.name) and not re.search(
                "environmentlogger", sensor.name, re.IGNORECASE
            ):
                sensors.append(sensor.name)
        return sensors


def get_crops(session, season, sensor, alt_layout):
    """A method to download the crops processed for a given season/sensor combo.
    and update them through Cyverse.

    Args:
        session (irodsSession): A session object that allows the program to query data
        from the Cyverse datastore.
        season (string): The actual name that appears on cyverse for the selected season
        sensor (string): The name of the sensor to be used.
        alt_layout (boolean): A bool to indicate if the user selected a season that
        has an alternate data organization layout.

    Returns:
        crops: A list of crops that were processed.
    """
    crops = []
    # Alt layout don't have a wrapper folder with the name of the crop
    if not alt_layout:
        try:
            season_crop_collection = session.collections.get(
                f"/iplant/home/shared/phytooracle/{season}/level_1/{sensor}"
            )
        except:
            st.write("No data for this sensor in this season. ")
            return crops
        else:
            for crop in season_crop_collection.subcollections:
                if not re.search("dep*", crop.name):
                    crops.append(crop.name)
            return crops
    return crops


def get_dates(session, season, sensor, crop):
    """A method to get a list of dates processed for a given
    season/sensor/crop combo from cyverse data store.

    Args:
        session (irodsSession): A session object that allows the program to query data
        from the Cyverse datastore.
        season (string): The actual name that appears on cyverse for the selected season
        sensor (string): The name of the sensor to be used.
        crop (string): The name of the crop to be accessed.

    Returns:
        dates (dict) : A dictionary that looks like this ->
        {'The date to be displayed (YYYY-MM-DD)': 'Actual date we got (contains time)'}
    """
    dates = {}
    try:
        if crop != "":
            season_date_collection = session.collections.get(
                f"/iplant/home/shared/phytooracle/{season}/level_1/{sensor}/{crop}/"
            )
        else:
            season_date_collection = session.collections.get(
                f"/iplant/home/shared/phytooracle/{season}/level_1/{sensor}/"
            )
    except:
        st.write(
            "This season's crop has not been processed yet for {sensor} sensor. Check back later."
        )
        return dates
    else:
        for directory in season_date_collection.subcollections:
            if not re.search("dep", directory.name):
                dates[directory.name.split("_")[0]] = directory.name
        return dates


def get_plant_detection_csv_path(
    session, season, sensor, crop, dates, selected_date, alt_layout
):
    """A method to get the location of the tarball that should
    have the plant detection csv. Only RGB and FLIR sensors have this

    Args:
        session (irodsSession): A session object that allows the program to query data
        from the Cyverse datastore.
        season (string): The actual name that appears on cyverse for the selected season
        sensor (string): The name of the sensor to be used.
        alt_layout (boolean): A bool to indicate if the user selected a season that
        has an alternate data organization layout.
        dates (dictionary): The dictionary returned by the get_dates() method.
        selected_date (string): The date selected by the user (no time)

    Returns:
        string: path
    """
    if re.search("3d", sensor, re.IGNORECASE):
        # Download CSV from the RGB sensor level 1
        dates_RGB = get_dates(session, season, "stereoTop", crop)
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
    date_directory = session.collections.get(date_directory_path)
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


def download_fieldbook(session, season):
    """Download the fieldbook for the specified season
    if it is not already in the cache.

    Args:
        session (irodsSession): A session object that allows the program to query data
        from the Cyverse datastore.
        season (string): The actual name that appears on cyverse for the selected season

    Returns:
        string: Name of the fieldbook file, nothing if nothing is found
    """
    season_file_collection = session.collections.get(
        f"/iplant/home/shared/phytooracle/{season}"
    )
    for file in season_file_collection.data_objects:
        if re.search("field*book", file.name, re.IGNORECASE) or re.search(
            "book", file.name, re.IGNORECASE
        ):
            if not (os.path.exists(f"field_books/{file.name}")):
                if not os.path.exists("field_books"):
                    os.makedirs("field_books")
                session.data_objects.get(
                    f"/iplant/home/shared/phytooracle/{season}/{file.name}",
                    "field_books",
                )
            return file.name
    return ""


def download_plant_detection_csv(session, local_file_name, plant_detection_csv_path):
    """Download and extract the plant detection csv from the file name
    Args:
        session (irodsSession): A session object that allows the program to query data
        from the Cyverse datastore.
        local_file_name (string): Name of the plant detection CSV if stored locally
        plant_detection_csv_path (string): Path on cyverse
    """
    if not (os.path.exists(f"detect_out/{local_file_name}")):
        session.data_objects.get(
            plant_detection_csv_path, "local_file_delete.tar", force=True
        )
        with tarfile.open("local_file_delete.tar", "r") as tar:
            tar.extractall()
        os.remove("local_file_delete.tar")


def data_analysis(
    session, season, plant_detect_df, field_book_name, sensor, crop, date, layout
):
    """Begin Analyzing the plant detection data. Start by making the fieldbook
    dataframe.
    Args:
        session (irodsSession): A session object that allows the program to query data
        from the Cyverse datastore.
        season (string): The actual name that appears on cyverse for the selected season
        plant_detect_df (dataframe): Pandas dataframe made using the plant detection CSV
        field_book_name (string): Name of the fielbook file.
        sensor (string): The name of the sensor to be used.
        layout (boolean): A bool to indicate if the user selected a season that
        has an alternate data organization layout.
        crop (string): Selected crop.
        date (string): Specified date.
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
    # DISCUSS THIS MERGE TECHNIQUE
    plant_detect_df = plant_detect_df.rename(columns={"Plot": "plot"})
    result = plant_detect_df.merge(field_book_df, on="plot")
    if "3D" in sensor or "ps2Top" in sensor:
        result = extra_processing(session, season, result, sensor, crop, date, layout)
    if not result.empty:
        create_filter(combined_data=result, sensor=sensor)


def extra_processing(session, season, combined_df, sensor, crop, date, alt_layout):
    """This part deals with downloading files and doing extra work for the
    3D and PSII sensors, that don't have geolocation data stored in one file.

    Args:
        session (irodsSession): A session object that allows the program to query data
        from the Cyverse datastore.
        season (string): The actual name that appears on cyverse for the selected season
        plant_detect_df (dataframe): Pandas dataframe made using the plant detection CSV
        combined_df: Pandas dataframe that is basically fieldbook + plant detection
        sensor (string): The name of the sensor to be used.
        alt_layout (boolean): A bool to indicate if the user selected a season that
        has an alternate data organization layout.
        crop (string): Selected crop.
        date (string): Specified date.
    """

    # NEED TO MAKE THIS WORK FOR ALT LAYOUT
    if "3D" in sensor:
        try:
            season_no = season.split("_")[1]
            download_extra_3D_data(session, season, season_no, sensor, crop, date)
            ind_plant_df = combine_all_csv(
                "3d_volumes_entropy_v009", sensor, crop, date
            )
            plant_clustering_df = pd.read_csv(
                f"plant_clustering/season_{season_no}_clustering.csv"
            ).loc[:, ["plant_name", "lat", "lon"]]
            combined_df = combined_df.merge(plant_clustering_df, on=["lat", "lon"])
            combined_df = combined_df.merge(ind_plant_df, on="plant_name")
            return combined_df
        except Exception as e:
            st.write(
                f"The 3D scans for this season were not processed to the level required for any visuals."
                f"Please try any other sensor/season. "
            )
            st.write(e)
            return pd.DataFrame()


def download_extra_3D_data(session, season, season_no, sensor, crop, date):
    if not (os.path.exists(f"3d_volumes_entropy_v009")):
        collection = session.collections.get(
            f"/iplant/home/shared/phytooracle/{season}/level_2/{sensor}/{crop}/{date}/individual_plants_out/"
        )
        for file in collection.data_objects:
            if re.search("volumes_entropy", file.name):
                session.data_objects.get(
                    f"/iplant/home/shared/phytooracle/{season}/level_2/{sensor}/"
                    f"{crop}/{date}/individual_plants_out/{file.name}",
                    "local_file_delete.tar",
                    force=True,
                )
                break
        with tarfile.open("local_file_delete.tar", "r") as tar:
            tar.extractall()
        os.remove("local_file_delete.tar")
    if not (os.path.exists(f"plant_clustering/season_{season_no}_clustering.csv")):
        if not os.path.exists("plant_clustering"):
            os.makedirs("plant_clustering")
        session.data_objects.get(
            f"/iplant/home/shared/phytooracle/{season}/level_2/stereoTop/"
            f"season_{season_no}_plant_detection_combined/"
            f"season_{season_no}_clustering.csv",
            f"plant_clustering/season_{season_no}_clustering.csv",
            force=True,
        )


def combine_all_csv(path, sensor, crop, date):
    """Combine all the CSVs in a directory into a single pandas dataframe
    Args:
        path (string):

    Returns:
        dataframe: combined data
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
        return result_frame
    else:
        return pd.read_csv(f"volumes_entropy/combined_csv_{sensor}-{crop}_{date}.csv")


def create_filter(combined_data, sensor):
    """Creates a dynamic fiter

    Args:
        combined_data (pandas df): Everything in this dataframe
        sensor (string): selected sensor
    """
    filter_options = []
    for column_name in combined_data.columns:
        if not re.search(f"lon|lat|max|min|date", column_name, re.IGNORECASE):
            filter_options.append(column_name)
    selected_column_name = filter_sec.selectbox("Filter", filter_options)
    col1.header("All Data")
    col1.dataframe(combined_data)
    selected_columns = []
    exact_column_name = selected_column_name
    for column_name in combined_data.columns:
        if re.search(
            f"{selected_column_name}|lon|lat|max|min", column_name, re.IGNORECASE
        ):
            if re.search(selected_column_name, column_name, re.IGNORECASE):
                exact_column_name = column_name
            selected_columns.append(column_name)
    filtered_df = combined_data.loc[:, combined_data.columns.isin(selected_columns)]
    col2.header("Filtered Data")
    col2.dataframe(filtered_df)
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
    get_visuals(filtered_df, exact_column_name)


def get_visuals(filtered_df, column_name):
    """Make the map plot, as well as the histogram based on the selected filed

    Args:
        filtered_df (_type_): Pandas df that has Co-ordinates +  selected field
        column_name (_type_): Selected column
    """
    # Emmanuel's API key, Might need to change this
    px.set_mapbox_access_token(
        "pk.eyJ1IjoiZW1tYW51ZWxnb256YWxleiIsImEiOiJja3RndzZ2NmIwbTJsMnBydGN1NWJ4bzkxIn0.rtptqiaoqpDIoXsw6Qa9lg"
    )
    fig = px.scatter_mapbox(
        filtered_df,
        lat="lat",
        lon="lon",
        color=column_name,
        zoom=16.6,
        opacity=1,
        mapbox_style="satellite-streets",
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

    dist = px.histogram(filtered_df, x=column_name, color=column_name)
    dist.update_layout(title=f"{column_name} distribution", autosize=True)
    dist_col.plotly_chart(dist, use_container_width=True)


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
                        #MainMenu {visibility: hidden; }
                        footer {visibility: hidden;}
                        </style>
                        """
    st.markdown(hide_default_format, unsafe_allow_html=True)
    st.sidebar.title(":green[Phytooracle] :seedling:")
    st.sidebar.subheader("Scan selection")
    st.title("Dashboard")
    global col1, col2, filter_sec, vis_container, plotly_col, dist_col
    filter_sec = st.container()
    vis_container = st.container()
    plotly_col, dist_col = vis_container.columns(2)
    col1, col2 = st.columns(2)
    # To establish an irods session
    try:
        session = iRODSSession(
            host="data.cyverse.org",
            port=1247,
            user="anonymous",
            password="anonymous",
            zone="iplant",
        )
    except:
        st.write("Something went wrong establishing a iRODS session. Contact support.")
    else:
        seasons = get_seasons(session)
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
            sensors = get_sensors(session, seasons[selected_season])
            if sensors:
                selected_sensor = st.sidebar.selectbox("Select a sensor: ", sensors)
                crops = get_crops(
                    session, seasons[selected_season], selected_sensor, alt_layout
                )
                if alt_layout or crops:
                    selected_crop = ""
                    if crops:
                        selected_crop = st.sidebar.selectbox(
                            "Select a crop: ", sorted(crops)
                        )
                dates = get_dates(
                    session, seasons[selected_season], selected_sensor, selected_crop
                )
                if dates:
                    display_dates = sorted(dates.keys())
                    selected_date = st.sidebar.selectbox(
                        "Select a date: ", display_dates
                    )
                    plant_detection_csv_path = get_plant_detection_csv_path(
                        session,
                        seasons[selected_season],
                        selected_sensor,
                        selected_crop,
                        dates,
                        selected_date,
                        alt_layout,
                    )
                    if plant_detection_csv_path != "":
                        # Download necessary files (just fieldbook and plantdetection csv for now)
                        with filter_sec:
                            with st.spinner(
                                "This might take some time. Please wait..."
                            ):
                                field_book_name = download_fieldbook(
                                    session, seasons[selected_season]
                                )
                                if field_book_name == "":
                                    st.write("No fieldbook for this season was found")
                                else:
                                    local_file_name = plant_detection_csv_path.split(
                                        "/"
                                    )[-1].split(".")[0]
                                    local_file_name = f"{local_file_name[: len(local_file_name) - 4]}ion.csv"
                                    local_file_name = (
                                        f"{dates[selected_date]}_fluorescence_aggregation"
                                        if selected_sensor == "ps2Top"
                                        else local_file_name
                                    )
                                    download_plant_detection_csv(
                                        session,
                                        local_file_name,
                                        plant_detection_csv_path,
                                    )
                                    plant_detect_df = (
                                        pd.read_csv(f"detect_out/{local_file_name}")
                                        if selected_sensor != "ps2Top"
                                        else pd.read_csv(
                                            f"fluorescence_aggregation_out/{dates[selected_date]}_fluorescence_aggregation.csv"
                                        )
                                    )
                                    # Data Analysis and vis section starts
                                    data_analysis(
                                        session,
                                        seasons[selected_season],
                                        plant_detect_df,
                                        field_book_name,
                                        selected_sensor,
                                        selected_crop,
                                        dates[selected_date],
                                        alt_layout,
                                    )


if __name__ == "__main__":
    main()
