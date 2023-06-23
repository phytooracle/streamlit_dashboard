from irods.session import iRODSSession
from datetime import datetime, timedelta
import pandas as pd
import streamlit as st
import re
import os
import tarfile
import plotly.express as px


@st.cache_data
def convert_df(df):
    # Cache the conversion to prevent computation on every rerun
    return df.to_csv(index=False).encode("utf-8")


def get_seasons(session):
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
            # might need to modify this for extra sensors - ASK
            if not re.search("depreceated", sensor.name) and not re.search(
                "environmentlogger", sensor.name, re.IGNORECASE
            ):
                sensors.append(sensor.name)
        return sensors


def get_crops(session, season, sensor, alt_layout):
    crops = []
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
    if re.search("3d", sensor, re.IGNORECASE):
        dates_RGB = get_dates(session, season, "stereoTop", crop)
        date_RGB = ""
        date_3D_obj = datetime.strptime(selected_date, "%Y-%m-%d")
        for date_id in dates_RGB.keys():
            dte_obj = datetime.strptime(date_id, "%Y-%m-%d")
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
    if not (os.path.exists(f"detect_out/{local_file_name}")):
        session.data_objects.get(
            plant_detection_csv_path, "local_file_delete.tar", force=True
        )
        with tarfile.open("local_file_delete.tar", "r") as tar:
            tar.extractall()
        os.remove("local_file_delete.tar")


def data_analysis(plant_detect_df, field_book_name, sensor):
    # make field book dataframe based on its extension
    if field_book_name.split(".")[1] == "xlsx":
        try:
            field_book_df = pd.read_excel(
                io=f"field_books/{field_book_name}", sheet_name="fieldbook"
            )
        except:
            field_book_df = pd.read_excel(
                io=f"field_books/{field_book_name}", sheet_name="Fieldbook"
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
    create_filter(combined_data=result, sensor=sensor)


def create_filter(combined_data, sensor):
    filter_options = {
        "ps2Top": ["Treatment", "Plot", "Rep", "Range", "Column", "Genotype", "FV/FM"],
        "stereoTop": [
            "Treatment",
            "Plot",
            "Rep",
            "Range",
            "Column",
            "Genotype",
            "Bounding_area_m2",
        ],
        "flirIrCamera": [
            "Treatment",
            "Plot",
            "Rep",
            "Range",
            "Column",
            "Genotype",
            "Median",
            "Mean",
            "Var",
            "std_dev",
        ],
        "scanner3DTop": [
            "Treatment",
            "Plot",
            "Rep",
            "Range",
            "Column",
            "Genotype",
            "axis_aligned_bounding_volume",
            "oriented_bounding_volume",
            "hull_volume",
        ],
    }
    selected_column_name = filter_sec.selectbox("Filter", filter_options[sensor])
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
                        with st.spinner("This might take some time. Please wait..."):
                            field_book_name = download_fieldbook(
                                session, seasons[selected_season]
                            )
                            if field_book_name == "":
                                st.write("No fieldbook for this season was found")
                            else:
                                local_file_name = plant_detection_csv_path.split("/")[
                                    -1
                                ].split(".")[0]
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
                                # pick up here
                                # Data Analysis and vis section starts
                                data_analysis(
                                    plant_detect_df, field_book_name, selected_sensor
                                )


if __name__ == "__main__":
    main()
