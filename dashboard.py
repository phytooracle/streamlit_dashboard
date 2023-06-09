from irods.session import iRODSSession
import pandas as pd
import streamlit as st
import re


# ---------------------------------------------------
def main():
    # A dictionary with (Season name, Directory Path)
    seasons = dict()
    dates = dict()

    # To establish a iRODS session
    session = iRODSSession(
        host="data.cyverse.org",
        port=1247,
        user="dummy",
        password="dummy",
        zone="iplant",
    )

    # To get a list of seasons
    root_collection = session.collections.get("/iplant/home/shared/phytooracle")

    for directory in root_collection.subcollections:
        if re.search("season*", directory.name):
            seasons["Season " + directory.name.split("_")[1]] = directory.name
    season_display_names = list(seasons.keys())
    season_display_names.sort(key=lambda x: int(x.split(" ")[1]))
    selected_season = st.selectbox("Select a season: ", season_display_names)

    season_date_collection = session.collections.get(
        "/iplant/home/shared/phytooracle/"
        + seasons[selected_season]
        + "/level_1/stereoTop/sorghum"
    )

    for directory in season_date_collection.subcollections:
        dates[directory.name.split("__")[0]] = directory.name
    display_dates = list(dates.keys())
    display_dates.sort()
    selected_date = st.selectbox("Select a date: ", display_dates)


# --------------------------------------------------
if __name__ == "__main__":
    main()
