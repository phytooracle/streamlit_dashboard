from irods.session import iRODSSession
import pandas as pd
import streamlit as st
import re
import os
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
        if not re.search("dep", directory.name):
            dates[directory.name.split("_")[0]] = directory.name
    return dates


def download_plant_detection_csv(
    session, dates, selected_date, plant_detection_csv_path
):
    progress_text = "Please wait. This might take a few minutes"
    my_bar = st.progress(0, text=progress_text)
    if not (os.path.exists(f"detect_out/{dates[selected_date]}_detection.csv")):
        for percent_complete in range(100):
            plant_detect_tar = session.data_objects.get(plant_detection_csv_path)
            with open("local_file.tar", "wb") as f:
                with plant_detect_tar.open("r") as d:
                    f.write(d.read())
                # Extract the contents of the tar file
                with tarfile.open("local_file.tar", "r") as tar:
                    tar.extractall()
            my_bar.progress(percent_complete + 1, text=progress_text)
    my_bar.empty()


def main():
    # To establish a iRODS session
    try:
        session = iRODSSession(
            host="data.cyverse.org",
            port=1247,
            user="adityakumar",
            password="adityakumar",
            zone="iplant",
        )
    except:
        st.write("Something went wrong establishing a iRODS session.")
    else:
        # Titles for sidebar and main section
        st.sidebar.title(":green[Phytooracle] :seedling:")
        st.title("Visualization")
        # To get a list of seasons
        try:
            seasons = get_seasons(session)
        except:
            st.write("No data present on Cyverse datastore for any season. ")
        else:
            season_display_names = sorted(
                seasons.keys(), key=lambda x: int(x.split(" ")[1])
            )
            selected_season = st.sidebar.selectbox(
                "Select a season: ", season_display_names
            )

            # To get a list of crops grown that season
            try:
                crops = get_crops(session, seasons[selected_season])
            except:
                st.write("No data for this season. ")
            else:
                selected_crop = st.sidebar.selectbox("Select a crop: ", sorted(crops))

                # To get a list of scans(dates) for the given crop
                try:
                    dates = get_dates(session, seasons[selected_season], selected_crop)
                except:
                    st.write(
                        f"This season's {selected_crop} has not been processed yet. Check back later."
                    )
                display_dates = sorted(dates.keys())
                selected_date = st.sidebar.selectbox("Select a date: ", display_dates)

                date_directory_path = f"/iplant/home/shared/phytooracle/{seasons[selected_season]}/level_1/stereoTop/{selected_crop}/{dates[selected_date]}"
                date_directory = session.collections.get(date_directory_path)

                # To go through the processed files for the date to finde the plant detection zip
                for files in date_directory.data_objects:
                    if "detect_out" in files.name:
                        plant_detection_csv_path = f"{date_directory_path}/{files.name}"

                download_plant_detection_csv(
                    session, dates, selected_date, plant_detection_csv_path
                )
                df = pd.read_csv(f"detect_out/{dates[selected_date]}_detection.csv")
                st.dataframe(df)


if __name__ == "__main__":
    main()
