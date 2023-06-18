from irods.session import iRODSSession
import pandas as pd
import streamlit as st
import re
import os
import tarfile


def create_filter(combined_data):
    st.dataframe(combined_data)
    selected_column_name = st.selectbox(
        "Filter", ["Treatment", "Plot", "Rep", "Range", "Column", "Genotype"]
    )
    # for column_name in combined_data.columns:
    #     if re.search(f"{selected_column_name}|lon|lat|max|min", column_name, re.IGNORECASE):


def data_analysis(plant_detect_df, field_book_name):
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
    result = plant_detect_df.merge(field_book_df, on="plot")
    create_filter(combined_data=result)


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
    if crop != "":
        season_date_collection = session.collections.get(
            f"/iplant/home/shared/phytooracle/{season}/level_1/stereoTop/{crop}/"
        )
    else:
        season_date_collection = session.collections.get(
            f"/iplant/home/shared/phytooracle/{season}/level_1/stereoTop/"
        )
    for directory in season_date_collection.subcollections:
        if not re.search("dep", directory.name):
            dates[directory.name.split("_")[0]] = directory.name
    return dates


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


def download_plant_detection_csv(
    session, dates, selected_date, plant_detection_csv_path
):
    if not (os.path.exists(f"detect_out/{dates[selected_date]}_detection.csv")):
        with st.spinner("This might take some time. Please wait..."):
            session.data_objects.get(
                plant_detection_csv_path, "local_file_delete.tar", force=True
            )
            with tarfile.open("local_file_delete.tar", "r") as tar:
                tar.extractall()
        os.remove("local_file_delete.tar")


def main():
    # To establish a iRODS session & configure the web app
    st.set_page_config(
        page_title="Phytooracle Dashboard", page_icon=":seedling:", layout="wide"
    )
    hide_default_format = """
                        <style>
                        .reportview-container.main.block-container {{
                        padding-top: {padding_top}rem;
                        }}
                        #MainMenu {visibility: hidden; }
                        footer {visibility: hidden;}
                        </style>
                        """
    st.markdown(hide_default_format, unsafe_allow_html=True)
    try:
        session = iRODSSession(
            host="data.cyverse.org",
            port=1247,
            user="adityakumar",
            password="adityakumar",
            zone="iplant",
        )
    except Exception as e:
        st.write("Something went wrong establishing a iRODS session.")
        st.write(e)
    else:
        # Titles for sidebar and main section
        st.sidebar.title(":green[Phytooracle] :seedling:")
        st.sidebar.subheader("Scan selection")
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
                if selected_season != "Season 10" or selected_season != "Season 11":
                    crops = get_crops(session, seasons[selected_season])
            except:
                st.write("No data for this season. ")
            else:
                if selected_season != "Season 10" and selected_season != "Season 11":
                    selected_crop = st.sidebar.selectbox(
                        "Select a crop: ", sorted(crops)
                    )
                # To get a list of scans(dates) for the given crop
                try:
                    if (
                        selected_season != "Season 10"
                        and selected_season != "Season 11"
                    ):
                        dates = get_dates(
                            session, seasons[selected_season], selected_crop
                        )
                    else:
                        dates = get_dates(session, seasons[selected_season], "")
                except:
                    st.write(
                        f"This season's crop has not been processed yet. Check back later."
                    )
                display_dates = sorted(dates.keys())
                selected_date = st.sidebar.selectbox("Select a date: ", display_dates)

                if selected_season != "Season 10" and selected_season != "Season 11":
                    date_directory_path = (
                        f"/iplant/home/shared/phytooracle/"
                        f"{seasons[selected_season]}/level_1/"
                        f"stereoTop/{selected_crop}/{dates[selected_date]}"
                    )
                else:
                    date_directory_path = (
                        f"/iplant/home/shared/phytooracle/"
                        f"{seasons[selected_season]}/level_1/"
                        f"stereoTop/{dates[selected_date]}"
                    )
                date_directory = session.collections.get(date_directory_path)
                # To go through the processed files for the date to finde the plant detection zip
                for files in date_directory.data_objects:
                    if "detect_out" in files.name:
                        plant_detection_csv_path = f"{date_directory_path}/{files.name}"

                # Download necessary files (just fieldbook and plantdetection csv for now)
                field_book_name = download_fieldbook(session, seasons[selected_season])
                if field_book_name == "":
                    st.write("No fieldbook for this season was found")
                else:
                    download_plant_detection_csv(
                        session, dates, selected_date, plant_detection_csv_path
                    )
                    plant_detect_df = pd.read_csv(
                        f"detect_out/{dates[selected_date]}_detection.csv"
                    )

                    # Data Analysis and vis section starts
                    data_analysis(plant_detect_df, field_book_name)


if __name__ == "__main__":
    main()
