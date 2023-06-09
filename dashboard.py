from irods.session import iRODSSession
import pandas as pd
import streamlit as st
import re
import tarfile


def get_seasons(session):
    seasons = {}
    root_collection = session.collections.get("/iplant/home/shared/phytooracle")
    for directory in root_collection.subcollections:
        if re.search("season*", directory.name):
            seasons["Season " + directory.name.split("_")[1]] = directory.name
    return seasons


def get_crops(session, season):
    crops = []
    season_crop_collection = session.collections.get(
        f"/iplant/home/shared/phytooracle/{season}/level_1/stereoTop"
    )
    for crop in season_crop_collection.subcollections:
        if not re.search("dep*", crop.name):
            crops.append(crop.name)
    return crops


def get_dates(session, season, crop):
    dates = {}
    season_date_collection = session.collections.get(
        f"/iplant/home/shared/phytooracle/{season}/level_1/stereoTop/{crop}/"
    )
    for directory in season_date_collection.subcollections:
        if not re.search("depreceated", directory.name):
            dates[directory.name.split("__")[0]] = directory.name
    return dates


def main():
    # To establish a iRODS session
    session = iRODSSession(
        host="data.cyverse.org",
        port=1247,
        user="adityakumar",
        password="adityakumar",
        zone="iplant",
    )

    # To get a list of seasons
    seasons = get_seasons(session)
    season_display_names = sorted(seasons.keys(), key=lambda x: int(x.split(" ")[1]))
    selected_season = st.selectbox("Select a season: ", season_display_names)

    # To get a list of crops grown that season
    crops = get_crops(session, seasons[selected_season])
    selected_crop = st.selectbox("Select a crop: ", sorted(crops))

    # To get a list of scans(dates) for the given crop
    dates = get_dates(session, seasons[selected_season], selected_crop)
    display_dates = sorted(dates.keys())
    selected_date = st.selectbox("Select a date: ", display_dates)

    date_directory_path = f"/iplant/home/shared/phytooracle/{seasons[selected_season]}/level_1/stereoTop/{selected_crop}/{dates[selected_date]}"

    date_directory = session.collections.get(date_directory_path)
    for files in date_directory.data_objects:
        if "detect_out" in files.name:
            plant_detection_csv_path = f"{date_directory_path}/{files.name}"

    plant_detect_tar = session.data_objects.get(plant_detection_csv_path)
    with open("local_file.tar", "wb") as f:
        with plant_detect_tar.open("r") as d:
            f.write(d.read())

    # Extract the contents of the tar file
    with tarfile.open("local_file.tar", "r") as tar:
        tar.extractall()

    print(f"detect_out/{dates[selected_date]}_detection.csv")
    df = pd.read_csv(f"detect_out/{dates[selected_date]}_detection.csv")
    st.dataframe(df)


if __name__ == "__main__":
    main()
