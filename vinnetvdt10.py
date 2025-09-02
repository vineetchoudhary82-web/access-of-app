import pandas as pd
import os
import streamlit as st
import logging
from datetime import datetime
import openpyxl
import math
from math import radians, sin, cos, sqrt, atan2
import webbrowser
import io
import requests
import tempfile
import base64
import numpy as np
import schedule
import time
from transformers import pipeline

# Configure logging
logging.basicConfig(filename='network_search.log', level=logging.ERROR,
                    format='%(asctime)s - %(levelname)s - %(message)s')

# Initialize AI model for query handling
nlp = pipeline("question-answering", model="distilbert-base-cased-distilled-squad")

class NetworkSearchApp:
    def __init__(self):
        # Initialize data structures in session state
        if 'lte_data' not in st.session_state:
            st.session_state.lte_data = pd.DataFrame()
        if 'nr_data' not in st.session_state:
            st.session_state.nr_data = pd.DataFrame()
        if 'bbu_data' not in st.session_state:
            st.session_state.bbu_data = pd.DataFrame()
        if 'file_paths' not in st.session_state:
            st.session_state.file_paths = []
        if 'matched_records' not in st.session_state:
            st.session_state.matched_records = []
        if 'points' not in st.session_state:
            st.session_state.points = []
        if 'master_point' not in st.session_state:
            st.session_state.master_point = None
        if 'vdt_data' not in st.session_state:
            st.session_state.vdt_data = pd.DataFrame(columns=["LTE Site", "NR Site"])
        if 'lte_columns' not in st.session_state:
            st.session_state.lte_columns = [
                "Source", "Site", "cell", "CELLRANGE", "CRSGAIN", "QRXLEVMIN", "EARFCNDL"
            ]
        if 'nr_columns' not in st.session_state:
            st.session_state.nr_columns = [
                "Source", "USID", "SITE", "NRCELL_NAME", "Digital Tilt", "Power", "PCI",
                "ADMINISTRATIVESTATE", "CELLBARRED", "CELLRESERVEDFOROPERATOR",
                "OPERATIONALSTATE", "CELLRANGE", "SSBFREQUENCY", "CONFIGURATION"
            ]

        # Initialize widget states
        if 'search_type' not in st.session_state:
            st.session_state.search_type = "USID"
        if 'search_value' not in st.session_state:
            st.session_state.search_value = ""
        if 'lte_cr_type' not in st.session_state:
            st.session_state.lte_cr_type = "cellRange"
        if 'nr_cr_type' not in st.session_state:
            st.session_state.nr_cr_type = "digitalTilt"
        if 'project_name' not in st.session_state:
            st.session_state.project_name = "ATT_STX_253"
        if 'map_type' not in st.session_state:
            st.session_state.map_type = "roadmap"
        if 'map_zoom' not in st.session_state:
            st.session_state.map_zoom = 12
        if 'auto_generate' not in st.session_state:
            st.session_state.auto_generate = True
        if 'distance_results' not in st.session_state:
            st.session_state.distance_results = ""
        if 'chat_history' not in st.session_state:
            st.session_state.chat_history = []

        # Market mapping
        self.market_mapping = {
            "ATT_ARK1": "ATT_ARK_253",
            "ATT_NoCAL1": "ATT_NoCAL_253",
            "ATT_SoCAL1": "ATT_SoCAL_253",
            "ATT_STX": "ATT_STX_253"
        }

        # Enhanced mappings
        self.mappings = {
            "LTE": {
                "USID": ["REMOTE_USID", "CSS_USID", "USID"],
                "ENBID": ["ENBID"],
                "cell ID": ["CELLID"],
                "Site": ["MECONTEXT_ID", "SITE", "OSS_ENodeB"],
                "Azumuth": ["ATOLL_AZIMUTH", "AZIMUTH", "Atoll_AZIMUT", "Atoll_Az"],
                "Digital Tilt": ["DIGITALTILT", "DIGITAL_TILT", "TILT"],
                "cell": ["EUTRAN_CELL_FDD_ID", "CELL", "OSS_EUTRAN_CELL_FDD_ID", "CELL_NAME"],
                "height(Meter)": ["Atoll_HEIGHT_m", "HEIGHT", "ANTENNA_HEIGHT", "HEIGHT_M"],
                "PCI": ["PHYSICALLAYERCELLID", "PCI", "OSS_PCI", "Atoll_PCI"],
                "Power": ["CONFIGUREDMAXTXPOWER", "TX_POWER", "OSS_CONFIGUREDMAXTXPOWER"],
                "LATITUDE": ["LATITUDE", "LAT"],
                "LONGITUDE": ["LONGITUDE", "LON", "LONG"],
                "ADMINISTRATIVESTATE": ["ADMINISTRATIVE_STATE", "ADMIN_STATE"],
                "OPERATIONALSTATE": ["OPERATIONALSTATE", "OP_STATE"],
                "CELLRANGE": ["CELLRANGE"],
                "CRSGAIN": ["CRSGAIN"],
                "QRXLEVMIN": ["QRXLEVMIN"],
                "EARFCNDL": ["EARFCNDL"],
                "Electrical Tilt": ["ELECTRICAL_TILT", "Atoll_ET", "E_TILT"],
                "ED_Market": ["ED_MARKET", "ED_Market", "EDMARKET"]
            },
            "5GNR": {
                "USID": ["CSS_USID", "REMOTE_USID", "USID", "BBU_USID"],
                "NIC": ["NCI"],
                "gnb ID": ["GNBID", "GNB_ID"],
                "Site": ["CTS_COMMON_ID", "GNB_NAME", "GNODEB"],
                "Azumuth": ["Atoll_AZIMUT", "AZIMUTH", "ATOLL_AZIMUTH"],
                "Digital Tilt": ["DIGITALTILT", "DIGITAL_TILT", "USEDDIGITALTILT"],
                "cell": ["NRCELLDUID", "CELL", "NRCELL_NAME"],
                "height(Meter)": ["Atoll_HEIGHT_m", "HEIGHT", "ANTENNA_HEIGHT", "ATOLL_ANTENNA_HEIGHT"],
                "PCI": ["NRPCI", "PCI"],
                "Power": ["CONFIGUREDMAXTXPOWER", "TX_POWER", "POWER"],
                "LATITUDE": ["LAT", "LATITUDE"],
                "LONGITUDE": ["LONG", "LONGITUDE"],
                "ADMINISTRATIVESTATE": ["ADMINISTRATIVESTATE", "ADMIN_STATE"],
                "OPERATIONALSTATE": ["OPERATIONALSTATE", "OP_STATE"],
                "CELLBARRED": ["CELLBARRED"],
                "CELLRESERVEDFOROPERATOR": ["CELLRESERVEDFOROPERATOR"],
                "CELLRANGE": ["CELLRANGE"],
                "SSBFREQUENCY": ["SSBFREQUENCY"],
                "ARFCNDL": ["ARFCNDL", "ARFCN_DL", "DL_ARFCN", "NR_ARFCNDL"],
                "CONFIGURATION": ["CONFIGURATION"],
                "Electrical Tilt": ["ELECTRICAL_TILT", "Atoll_ET", "E_TILT"],
                "ED_Market": ["ED_MARKET", "ED_Market", "EDMARKET"],
                "BBU_TECH": ["BBU_TECH"],
                "GNB_SA_STATE": ["GNB_SA_STATE"],
                "CELL_SA_STATE": ["CELL_SA_STATE"],
                "CELL_TYPE": ["CELL_TYPE"],
                "ON_AIR": ["ON_AIR"],
                "DSS_LTECELL": ["DSS_LTECELL"],
                "NRTAC": ["NRTAC"]
            },
            "5GNR_BBU": {
                "USID": ["USID", "REMOTE_USID", "CSS_USID"],
                "NRCELL_NAME": ["NRCELLDUID", "CELL", "NRCELL_NAME"],
                "CONFIGURATION": ["CONFIGURATION"],
                "ED_Market": ["ED_MARKET", "ED_Market", "EDMARKET"]
            }
        }

        # Authentication credentials
        self.username = "your_username"
        self.password = "your_password"

        # Path for automatic upload
        self.upload_folder = "C:/DataFolder"  # Update to your folder path
        self.upload_time = "01:00"  # Daily upload time (e.g., 1:00 AM IST)

        # Custom CSS for styling
        st.markdown(
            """
            <style>
            .stApp {
                background-color: #f0f4f8;
                color: #333;
            }
            .stButton>button {
                background-color: #1e90ff;
                color: white;
                border-radius: 5px;
                padding: 5px 15px;
                transition: all 0.3s;
            }
            .stButton>button:hover {
                background-color: #104e8b;
                transform: scale(1.05);
            }
            .stHeader {
                font-size: 24px;
                font-weight: bold;
                color: #1e90ff;
                margin-bottom: 10px;
            }
            .collapsible {
                cursor: pointer;
                padding: 10px;
                background-color: #e6f0fa;
                border-radius: 5px;
                margin-bottom: 5px;
            }
            .content {
                padding: 0 10px;
                display: none;
            }
            .content.show {
                display: block;
            }
            @media (max-width: 600px) {
                .stContainer {
                    padding: 5px;
                }
                .stButton>button {
                    width: 100%;
                    margin-bottom: 5px;
                }
            }
            </style>
            """,
            unsafe_allow_html=True
        )

        # Create UI
        self.authenticate()
        self.create_widgets()
        self.schedule_upload()

    def authenticate(self):
        """Authenticate user to restrict data uploads"""
        if 'authenticated' not in st.session_state:
            st.session_state.authenticated = False
        if not st.session_state.authenticated:
            st.markdown("<h1 class='stHeader'>Network Data Search Tool</h1>", unsafe_allow_html=True)
            username = st.text_input("Username", key="auth_username")
            password = st.text_input("Password", type="password", key="auth_password")
            if st.button("Login \uF023", help="Login to access the app"):
                if username == self.username and password == self.password:
                    st.session_state.authenticated = True
                    st.success("Login successful!")
                else:
                    st.error("Invalid credentials")
            st.stop()
        else:
            st.sidebar.success("Logged in as: " + self.username)

    def update_status(self, message):
        """Update status with timestamp"""
        timestamp = datetime.now().strftime("%H:%M:%S")
        st.session_state.status = f"[{timestamp}] {message}"
        st.write(st.session_state.status)

    def create_widgets(self):
        """Create Streamlit UI with enhanced layout"""
        st.markdown("<h1 class='stHeader'>Network Data Search Tool</h1>", unsafe_allow_html=True)
        progress_bar = st.progress(0)

        # Data Loading Section
        with st.container():
            st.markdown("<div class='collapsible' onclick=\"this.nextElementSibling.classList.toggle('show')\">Data Loading \u25BC</div>", unsafe_allow_html=True)
            with st.container() as content:
                st.markdown("<div class='content'>", unsafe_allow_html=True)
                if st.session_state.authenticated:
                    uploaded_files = st.file_uploader("Upload Multiple Files", accept_multiple_files=True, type=["xlsx", "xls"])
                    if uploaded_files:
                        st.session_state.file_paths = [file.name for file in uploaded_files]
                        st.write("Uploaded Files:", ", ".join(st.session_state.file_paths))
                    col1, col2 = st.columns([1, 1])
                    with col1:
                        if st.button("Load Selected Data \uF019"):
                            progress_bar.progress(50)
                            self.load_data(uploaded_files)
                            progress_bar.progress(100)
                    with col2:
                        st.write("Auto-upload enabled at " + self.upload_time + " daily")
                else:
                    st.warning("Only authenticated users can upload files. Please log in.")
                st.markdown("</div>", unsafe_allow_html=True)

        # AI Chat Section
        with st.container():
            st.markdown("<div class='collapsible' onclick=\"this.nextElementSibling.classList.toggle('show')\">AI Assistant \u25BC</div>", unsafe_allow_html=True)
            with st.container() as content:
                st.markdown("<div class='content'>", unsafe_allow_html=True)
                chat_input = st.text_input("Ask me anything (e.g., 'search for USID 123')", key="chat_input")
                if chat_input:
                    self.handle_ai_query(chat_input)
                    for msg in st.session_state.chat_history:
                        st.write(msg)
                st.markdown("</div>", unsafe_allow_html=True)

        # Search Section
        with st.container():
            st.markdown("<div class='collapsible' onclick=\"this.nextElementSibling.classList.toggle('show')\">Search \u25BC</div>", unsafe_allow_html=True)
            with st.container() as content:
                st.markdown("<div class='content'>", unsafe_allow_html=True)
                col1, col2, col3 = st.columns([1, 2, 1])
                with col1:
                    st.selectbox("Search By:", ["USID", "NIC", "gnb ID", "ENBID", "cell ID", "Site"], key="search_type")
                with col2:
                    st.text_input("Value:", key="search_value")
                with col3:
                    if st.button("Search \uF002"):
                        self.perform_search()
                st.markdown("</div>", unsafe_allow_html=True)

        # Tabs
        tabs = st.tabs(["Main Results", "LTE Parameters", "5G Parameters", "VDT Sheet", "Distance Calculator"])
        with tabs[0]:
            self.create_main_tab()
        with tabs[1]:
            self.create_lte_tab()
        with tabs[2]:
            self.create_5g_tab()
        with tabs[3]:
            self.create_vdt_tab()
        with tabs[4]:
            self.create_distance_tab()

    def create_main_tab(self):
        """Create main results tab"""
        st.markdown("<h2 class='stHeader'>Main Results</h2>", unsafe_allow_html=True)
        columns = [
            "Source", "NIC", "gnb ID", "ENBID", "cell ID", "USID", "Site",
            "Azumuth", "Digital Tilt", "cell", "height(Meter)", "PCI", "Power",
            "LATITUDE", "LONGITUDE", "ADMINISTRATIVESTATE", "OPERATIONALSTATE"
        ]
        if st.session_state.matched_records:
            main_data = []
            for tech, record in st.session_state.matched_records:
                row = {"Source": tech}
                for key in self.mappings[tech]:
                    row[key] = self.get_column_value(record, self.mappings[tech][key], tech)
                main_data.append(row)
            df = pd.DataFrame(main_data, columns=columns)
            st.dataframe(df, use_container_width=True)
            col1, col2 = st.columns([1, 1])
            with col1:
                if st.button("Export to Excel \uF019"):
                    self.export_to_excel(df, "Main_Results.xlsx")
            with col2:
                if st.button("Clear Results \uF12D"):
                    self.clear_results()

    def create_lte_tab(self):
        """Create LTE parameters tab"""
        st.markdown("<h2 class='stHeader'>LTE Parameters</h2>", unsafe_allow_html=True)
        col1, col2, col3, col4 = st.columns([1, 1, 1, 1])
        with col1:
            if st.button("Add Column \uF067"):
                self.add_lte_column()
        with col2:
            st.selectbox("Generate CR for:", ["cellRange", "crsGain", "Electrical Tilt"], key="lte_cr_type")
        with col3:
            if st.button("Generate CR \uF0C7"):
                self.generate_lte_cr()
        with col4:
            if st.button("Export LTE Data to Excel \uF019"):
                self.export_lte_to_excel()
        if st.session_state.matched_records:
            lte_data = []
            for tech, record in st.session_state.matched_records:
                if tech == "LTE":
                    row = {
                        "Source": "LTE",
                        "Site": self.get_column_value(record, self.mappings["LTE"]["Site"], "LTE"),
                        "cell": self.get_column_value(record, self.mappings["LTE"]["cell"], "LTE"),
                        "CELLRANGE": self.get_column_value(record, self.mappings["LTE"]["CELLRANGE"], "LTE"),
                        "CRSGAIN": self.get_column_value(record, self.mappings["LTE"]["CRSGAIN"], "LTE"),
                        "QRXLEVMIN": self.get_column_value(record, self.mappings["LTE"]["QRXLEVMIN"], "LTE"),
                        "EARFCNDL": self.get_column_value(record, self.mappings["LTE"]["EARFCNDL"], "LTE")
                    }
                    for col in st.session_state.lte_columns:
                        if col not in row:
                            row[col] = self.get_column_value(record, [col], "LTE")
                    lte_data.append(row)
            if lte_data:
                df = pd.DataFrame(lte_data, columns=st.session_state.lte_columns)
                st.dataframe(df, use_container_width=True)
                if st.button("Export Selected to Excel \uF019"):
                    self.export_lte_selected_to_excel(df)

    def create_5g_tab(self):
        """Create 5G parameters tab"""
        st.markdown("<h2 class='stHeader'>5G Parameters</h2>", unsafe_allow_html=True)
        col1, col2, col3, col4 = st.columns([1, 1, 1, 1])
        with col1:
            if st.button("Add Column \uF067"):
                self.add_5g_column()
        with col2:
            st.selectbox("Generate CR for:", ["digitalTilt", "cellRange"], key="nr_cr_type")
        with col3:
            if st.button("Generate 5G CR \uF0C7"):
                self.generate_5g_cr()
        with col4:
            if st.button("Export 5G Data to Excel \uF019"):
                self.export_5g_to_excel()
        if st.session_state.matched_records:
            nr_data = []
            for tech, record in st.session_state.matched_records:
                if tech == "5GNR":
                    row = {
                        "Source": "5GNR",
                        "USID": self.get_column_value(record, self.mappings["5GNR"]["USID"], "5GNR"),
                        "SITE": self.get_column_value(record, self.mappings["5GNR"]["Site"], "5GNR"),
                        "NRCELL_NAME": self.get_column_value(record, self.mappings["5GNR"]["cell"], "5GNR"),
                        "Digital Tilt": self.get_column_value(record, self.mappings["5GNR"]["Digital Tilt"], "5GNR"),
                        "Power": self.get_column_value(record, self.mappings["5GNR"]["Power"], "5GNR"),
                        "PCI": self.get_column_value(record, self.mappings["5GNR"]["PCI"], "5GNR"),
                        "ADMINISTRATIVESTATE": self.get_column_value(record, self.mappings["5GNR"]["ADMINISTRATIVESTATE"], "5GNR"),
                        "CELLBARRED": self.get_column_value(record, self.mappings["5GNR"]["CELLBARRED"], "5GNR"),
                        "CELLRESERVEDFOROPERATOR": self.get_column_value(record, self.mappings["5GNR"]["CELLRESERVEDFOROPERATOR"], "5GNR"),
                        "OPERATIONALSTATE": self.get_column_value(record, self.mappings["5GNR"]["OPERATIONALSTATE"], "5GNR"),
                        "CELLRANGE": self.get_column_value(record, self.mappings["5GNR"]["CELLRANGE"], "5GNR"),
                        "SSBFREQUENCY": self.get_column_value(record, self.mappings["5GNR"]["SSBFREQUENCY"], "5GNR"),
                        "CONFIGURATION": self.get_column_value(record, self.mappings["5GNR"]["CONFIGURATION"], "5GNR")
                    }
                    for col in st.session_state.nr_columns:
                        if col not in row:
                            row[col] = self.get_column_value(record, [col], "5GNR")
                    nr_data.append(row)
            if nr_data:
                df = pd.DataFrame(nr_data, columns=st.session_state.nr_columns)
                st.dataframe(df, use_container_width=True)
                if st.button("Export Selected to Excel \uF019"):
                    self.export_5g_selected_to_excel(df)

    def create_vdt_tab(self):
        """Create VDT sheet tab"""
        st.markdown("<h2 class='stHeader'>VDT Sheet</h2>", unsafe_allow_html=True)
        col1, col2, col3 = st.columns([1, 1, 1])
        with col1:
            st.selectbox("Project Name:", list(self.market_mapping.values()), key="project_name")
        with col2:
            if st.button("Generate VDT Report \uF0C7"):
                self.generate_vdt_report()
        with col3:
            st.checkbox("Auto-Generate on Search", value=st.session_state.auto_generate, key="auto_generate")
        if not st.session_state.vdt_data.empty:
            st.dataframe(st.session_state.vdt_data, use_container_width=True)

    def create_distance_tab(self):
        """Create distance calculator tab"""
        st.markdown("<h2 class='stHeader'>Distance Calculator</h2>", unsafe_allow_html=True)
        col1, col2 = st.columns([1, 2])
        with col1:
            st.write("Add Point")
            point_name = st.text_input("Point Name:", key="point_name")
            lat = st.number_input("Latitude:", format="%.6f", key="lat")
            lon = st.number_input("Longitude:", format="%.6f", key="lon")
            if st.button("Add Point \uF041"):
                self.add_point(point_name, lat, lon)
            st.write("Add from Lat,Long")
            lat_long = st.text_input("Lat,Long (e.g., 12.34,78.56):", key="lat_long")
            lat_long_name = st.text_input("Point Name for Lat,Long:", key="lat_long_name")
            if st.button("Add Lat,Long \uF041"):
                self.add_lat_long(lat_long, lat_long_name)
            uploaded_points = st.file_uploader("Import Points from Excel", type=["xlsx", "xls"], key="points_upload")
            if uploaded_points:
                self.import_points_from_excel(uploaded_points)
            if st.session_state.points:
                st.write("Points List:")
                points_df = pd.DataFrame(st.session_state.points, columns=["Name", "Latitude", "Longitude"])
                selected = st.multiselect("Select Points:", points_df["Name"].tolist(), key="points_select")
                col_a, col_b, col_c = st.columns([1, 1, 1])
                with col_a:
                    if st.button("Set as Master \uF3C5"):
                        if selected:
                            point = next(p for p in st.session_state.points if p[0] == selected[0])
                            self.set_master_point(point)
                with col_b:
                    if st.button("Remove Selected \uF1F8"):
                        self.remove_points(selected)
                with col_c:
                    if st.button("Clear All Points \uF12D"):
                        self.clear_points()
        with col2:
            dist_tabs = st.tabs(["Results", "Map Visualization"])
            with dist_tabs[0]:
                col_d, col_e = st.columns([1, 1])
                with col_d:
                    if st.button("Calculate Path Distances \uF6F6"):
                        self.calculate_path_distances()
                with col_e:
                    if st.button("Calculate from Master \uF6F6"):
                        self.calculate_from_master()
                if st.session_state.distance_results:
                    st.text_area("Distance Results:", st.session_state.distance_results, height=300, key="distance_results")
            with dist_tabs[1]:
                col_f, col_g = st.columns([1, 1])
                with col_f:
                    st.selectbox("Map Type:", ["roadmap", "satellite", "hybrid", "terrain"], key="map_type")
                with col_g:
                    st.number_input("Zoom:", min_value=1, max_value=20, value=st.session_state.map_zoom, key="map_zoom")
                if st.button("Show Map \uF5A0"):
                    self.show_map()
                if st.session_state.points:
                    st.write("Map Legend:")
                    for i, (name, _, _) in enumerate(st.session_state.points):
                        st.write(f"{chr(65 + i)}: {name}")
                    if st.session_state.master_point:
                        st.write(f"Master Point: {st.session_state.master_point[0]} (blue)")

    def load_data(self, files):
        """Load data from uploaded files"""
        try:
            new_lte_data = []
            new_nr_data = []
            new_bbu_data = []
            for file in files:
                df = pd.read_excel(file)
                file_name = file.name.lower()
                if "lte" in file_name:
                    new_lte_data.append(df)
                elif "nr" in file_name:
                    new_nr_data.append(df)
                elif "bbu" in file_name:
                    new_bbu_data.append(df)
            st.session_state.lte_data = pd.concat(new_lte_data, ignore_index=True) if new_lte_data else pd.DataFrame()
            st.session_state.nr_data = pd.concat(new_nr_data, ignore_index=True) if new_nr_data else pd.DataFrame()
            st.session_state.bbu_data = pd.concat(new_bbu_data, ignore_index=True) if new_bbu_data else pd.DataFrame()
            self.update_status(f"Loaded {len(files)} files")
            st.success("Data loading completed!")
        except Exception as e:
            logging.error(f"Error in load_data: {str(e)}")
            self.update_status(f"Error loading files: {str(e)}")
            st.error(f"Error loading files: {str(e)}")

    def get_column_value(self, record, possible_names, tech):
        """Get column value from record using possible names"""
        for name in possible_names:
            if name in record and pd.notna(record[name]):
                return str(record[name])
        if tech == "5GNR" and possible_names[0] == "CONFIGURATION":
            usid = self.get_column_value(record, self.mappings["5GNR"]["USID"], "5GNR")
            nrcell = self.get_column_value(record, self.mappings["5GNR"]["cell"], "5GNR")
            if usid and nrcell:
                key = f"{usid}_{nrcell}"
                bbu_row = st.session_state.bbu_data[
                    (st.session_state.bbu_data[self.mappings["5GNR_BBU"]["USID"]].apply(lambda x: str(x)) == usid) &
                    (st.session_state.bbu_data[self.mappings["5GNR_BBU"]["NRCELL_NAME"]].apply(lambda x: str(x)) == nrcell)
                ]
                if not bbu_row.empty:
                    for name in self.mappings["5GNR_BBU"]["CONFIGURATION"]:
                        if name in bbu_row.iloc[0] and pd.notna(bbu_row.iloc[0][name]):
                            return str(bbu_row.iloc[0][name])
        return ""

    def perform_search(self):
        """Perform search based on user input"""
        if not st.session_state.search_value:
            st.error("Please enter a search value")
            return
        new_matched_records = []
        main_data = []
        lte_data = []
        nr_data = []
        def search_in_data(data, tech):
            mapping = self.mappings[tech][st.session_state.search_type]
            for _, record in data.iterrows():
                for col_name in mapping:
                    if col_name in record and pd.notna(record[col_name]) and \
                       st.session_state.search_value.lower() in str(record[col_name]).lower():
                        new_matched_records.append((tech, record.to_dict()))
                        row_data = {"Source": tech}
                        for key in self.mappings[tech]:
                            row_data[key] = self.get_column_value(record, self.mappings[tech][key], tech)
                        main_data.append(row_data)
                        if tech == "LTE":
                            lte_row = {
                                "Source": tech,
                                "Site": row_data["Site"],
                                "cell": row_data["cell"],
                                "CELLRANGE": row_data["CELLRANGE"],
                                "CRSGAIN": row_data["CRSGAIN"],
                                "QRXLEVMIN": row_data["QRXLEVMIN"],
                                "EARFCNDL": row_data["EARFCNDL"]
                            }
                            for col in st.session_state.lte_columns:
                                if col not in lte_row:
                                    lte_row[col] = self.get_column_value(record, [col], tech)
                            lte_data.append(lte_row)
                        elif tech == "5GNR":
                            nr_row = {
                                "Source": tech,
                                "USID": row_data["USID"],
                                "SITE": row_data["Site"],
                                "NRCELL_NAME": row_data["cell"],
                                "Digital Tilt": row_data["Digital Tilt"],
                                "Power": row_data["Power"],
                                "PCI": row_data["PCI"],
                                "ADMINISTRATIVESTATE": row_data["ADMINISTRATIVESTATE"],
                                "CELLBARRED": row_data["CELLBARRED"],
                                "CELLRESERVEDFOROPERATOR": row_data["CELLRESERVEDFOROPERATOR"],
                                "OPERATIONALSTATE": row_data["OPERATIONALSTATE"],
                                "CELLRANGE": row_data["CELLRANGE"],
                                "SSBFREQUENCY": row_data["SSBFREQUENCY"],
                                "CONFIGURATION": row_data["CONFIGURATION"]
                            }
                            for col in st.session_state.nr_columns:
                                if col not in nr_row:
                                    nr_row[col] = self.get_column_value(record, [col], "5GNR")
                            nr_data.append(nr_row)
                        break
        if not st.session_state.lte_data.empty:
            search_in_data(st.session_state.lte_data, "LTE")
        if not st.session_state.nr_data.empty:
            search_in_data(st.session_state.nr_data, "5GNR")
        st.session_state.matched_records = new_matched_records
        if st.session_state.auto_generate:
            self.generate_vdt_data(lte_data, nr_data)
        self.update_status(f"Found {len(new_matched_records)} matching records")
        st.success(f"Found {len(new_matched_records)} matching records")

    def generate_vdt_data(self, lte_rows, nr_rows):
        """Generate VDT data"""
        lte_sites = sorted(set(row["Site"] for row in lte_rows if row["Site"]))
        nr_sites = sorted(set(row["SITE"] for row in nr_rows if row["SITE"]))
        max_len = max(len(lte_sites), len(nr_sites))
        vdt_rows = []
        for i in range(max_len):
            vdt_rows.append({
                "LTE Site": lte_sites[i] if i < len(lte_sites) else "",
                "NR Site": nr_sites[i] if i < len(nr_sites) else ""
            })
        st.session_state.vdt_data = pd.DataFrame(vdt_rows)
        self.update_status(f"Generated VDT data with {len(vdt_rows)} rows")

    def export_to_excel(self, df, filename):
        """Export DataFrame to Excel"""
        try:
            output = io.BytesIO()
            df.to_excel(output, index=False)
            output.seek(0)
            st.download_button(
                label=f"Download {filename} \uF019",
                data=output,
                file_name=filename,
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            )
            self.update_status(f"Exported {len(df)} rows to {filename}")
        except Exception as e:
            logging.error(f"Error in export_to_excel: {str(e)}")
            st.error(f"Export failed: {str(e)}")

    def export_lte_to_excel(self):
        """Export LTE data to Excel"""
        lte_data = [
            {
                "Source": "LTE",
                "Site": self.get_column_value(record, self.mappings["LTE"]["Site"], "LTE"),
                "cell": self.get_column_value(record, self.mappings["LTE"]["cell"], "LTE"),
                "CELLRANGE": self.get_column_value(record, self.mappings["LTE"]["CELLRANGE"], "LTE"),
                "CRSGAIN": self.get_column_value(record, self.mappings["LTE"]["CRSGAIN"], "LTE"),
                "QRXLEVMIN": self.get_column_value(record, self.mappings["LTE"]["QRXLEVMIN"], "LTE"),
                "EARFCNDL": self.get_column_value(record, self.mappings["LTE"]["EARFCNDL"], "LTE")
            } for tech, record in st.session_state.matched_records if tech == "LTE"
        ]
        if not lte_data:
            st.error("No LTE data to export")
            return
        df = pd.DataFrame(lte_data, columns=st.session_state.lte_columns)
        self.export_to_excel(df, "LTE_Parameters.xlsx")

    def export_5g_to_excel(self):
        """Export 5G data to Excel"""
        nr_data = [
            {
                "Source": "5GNR",
                "USID": self.get_column_value(record, self.mappings["5GNR"]["USID"], "5GNR"),
                "SITE": self.get_column_value(record, self.mappings["5GNR"]["Site"], "5GNR"),
                "NRCELL_NAME": self.get_column_value(record, self.mappings["5GNR"]["cell"], "5GNR"),
                "Digital Tilt": self.get_column_value(record, self.mappings["5GNR"]["Digital Tilt"], "5GNR"),
                "Power": self.get_column_value(record, self.mappings["5GNR"]["Power"], "5GNR"),
                "PCI": self.get_column_value(record, self.mappings["5GNR"]["PCI"], "5GNR"),
                "ADMINISTRATIVESTATE": self.get_column_value(record, self.mappings["5GNR"]["ADMINISTRATIVESTATE"], "5GNR"),
                "CELLBARRED": self.get_column_value(record, self.mappings["5GNR"]["CELLBARRED"], "5GNR"),
                "CELLRESERVEDFOROPERATOR": self.get_column_value(record, self.mappings["5GNR"]["CELLRESERVEDFOROPERATOR"], "5GNR"),
                "OPERATIONALSTATE": self.get_column_value(record, self.mappings["5GNR"]["OPERATIONALSTATE"], "5GNR"),
                "CELLRANGE": self.get_column_value(record, self.mappings["5GNR"]["CELLRANGE"], "5GNR"),
                "SSBFREQUENCY": self.get_column_value(record, self.mappings["5GNR"]["SSBFREQUENCY"], "5GNR"),
                "CONFIGURATION": self.get_column_value(record, self.mappings["5GNR"]["CONFIGURATION"], "5GNR")
            } for tech, record in st.session_state.matched_records if tech == "5GNR"
        ]
        if not nr_data:
            st.error("No 5G data to export")
            return
        df = pd.DataFrame(nr_data, columns=st.session_state.nr_columns)
        self.export_to_excel(df, "5G_Parameters.xlsx")

    def export_lte_selected_to_excel(self, df):
        """Export selected LTE rows to Excel"""
        if df.empty:
            st.error("No rows selected for export")
            return
        self.export_to_excel(df, "LTE_Selected.xlsx")

    def export_5g_selected_to_excel(self, df):
        """Export selected 5G rows to Excel"""
        if df.empty:
            st.error("No rows selected for export")
            return
        self.export_to_excel(df, "5G_Selected.xlsx")

    def generate_lte_cr(self):
        """Generate LTE CR Excel"""
        lte_data = [
            {
                "Source": "LTE",
                "Site": self.get_column_value(record, self.mappings["LTE"]["Site"], "LTE"),
                "cell": self.get_column_value(record, self.mappings["LTE"]["cell"], "LTE"),
                "CELLRANGE": self.get_column_value(record, self.mappings["LTE"]["CELLRANGE"], "LTE"),
                "CRSGAIN": self.get_column_value(record, self.mappings["LTE"]["CRSGAIN"], "LTE"),
                "QRXLEVMIN": self.get_column_value(record, self.mappings["LTE"]["QRXLEVMIN"], "LTE"),
                "EARFCNDL": self.get_column_value(record, self.mappings["LTE"]["EARFCNDL"], "LTE"),
                "Electrical Tilt": self.get_column_value(record, self.mappings["LTE"]["Electrical Tilt"], "LTE")
            } for tech, record in st.session_state.matched_records if tech == "LTE"
        ]
        if not lte_data:
            st.error("No LTE rows selected")
            return
        cr_data = []
        for row in lte_data:
            site = row["Site"]
            cell = row["cell"]
            value = ""
            mo_class = f"EUtranCellFDD={cell}"
            if st.session_state.lte_cr_type == "cellRange":
                value = row["CELLRANGE"]
            elif st.session_state.lte_cr_type == "crsGain":
                value = row["CRSGAIN"]
            elif st.session_state.lte_cr_type == "Electrical Tilt":
                value = row["Electrical Tilt"]
            cr_data.append({
                "Site": site,
                "MO Class": mo_class,
                "Parameter": st.session_state.lte_cr_type,
                "Value": value,
                "CurrentValue": ""
            })
        if cr_data:
            df = pd.DataFrame(cr_data)
            self.export_to_excel(df, "LTE_CR.xlsx")
            self.update_status(f"Generated {len(cr_data)} LTE CRs for {st.session_state.lte_cr_type}")

    def generate_5g_cr(self):
        """Generate 5G CR Excel"""
        nr_data = [
            {
                "Source": "5GNR",
                "USID": self.get_column_value(record, self.mappings["5GNR"]["USID"], "5GNR"),
                "SITE": self.get_column_value(record, self.mappings["5GNR"]["Site"], "5GNR"),
                "NRCELL_NAME": self.get_column_value(record, self.mappings["5GNR"]["cell"], "5GNR"),
                "Digital Tilt": self.get_column_value(record, self.mappings["5GNR"]["Digital Tilt"], "5GNR"),
                "Power": self.get_column_value(record, self.mappings["5GNR"]["Power"], "5GNR"),
                "PCI": self.get_column_value(record, self.mappings["5GNR"]["PCI"], "5GNR"),
                "CELLRANGE": self.get_column_value(record, self.mappings["5GNR"]["CELLRANGE"], "5GNR")
            } for tech, record in st.session_state.matched_records if tech == "5GNR"
        ]
        if not nr_data:
            st.error("No 5G rows selected")
            return
        cr_data = []
        for row in nr_data:
            site = row["SITE"]
            cell = row["NRCELL_NAME"]
            value = ""
            mo_class = f"NRCellDU={cell}" if st.session_state.nr_cr_type == "cellRange" else f"NRSectorCarrier={cell},CommonBeamforming=1"
            if st.session_state.nr_cr_type == "digitalTilt":
                value = row["Digital Tilt"]
            elif st.session_state.nr_cr_type == "cellRange":
                value = row["CELLRANGE"]
            cr_data.append({
                "Site": site,
                "MO Class": mo_class,
                "Parameter": st.session_state.nr_cr_type,
                "Value": value,
                "CurrentValue": ""
            })
        if cr_data:
            df = pd.DataFrame(cr_data)
            self.export_to_excel(df, "5G_CR.xlsx")
            self.update_status(f"Generated {len(cr_data)} 5G CRs for {st.session_state.nr_cr_type}")

    def generate_vdt_report(self):
        """Generate VDT report Excel"""
        lte_sites = sorted(set(row["Site"] for row in [
            {
                "Site": self.get_column_value(record, self.mappings["LTE"]["Site"], "LTE")
            } for tech, record in st.session_state.matched_records if tech == "LTE"
        ] if row["Site"]))
        nr_sites = sorted(set(row["SITE"] for row in [
            {
                "SITE": self.get_column_value(record, self.mappings["5GNR"]["Site"], "5GNR")
            } for tech, record in st.session_state.matched_records if tech == "5GNR"
        ] if row["SITE"]))
        if not lte_sites and not nr_sites:
            st.error("No sites found in VDT tab. Please generate VDT data first.")
            return
        wb = openpyxl.Workbook()
        # LTE Sheet
        lte_data = [
            ["Type", "Value", "Remarks", "Buffer", ""],
            ["projectName", st.session_state.project_name, "", "0.01", ""],
            ["startTime", datetime.now().strftime("%Y-%m-%d %H:%M:%S"), "Use exact format. Add \" ' \" before", "Default keep it 0.01. Can increase it to accommodate TA Plot", ""],
            ["endTime", datetime.now().strftime("%Y-%m-%d %H:%M:%S"), "Use exact format. Add \" ' \" before", "", ""],
            ["Site Name List", "ZOOM (Default = 0 for automatic zoom, enter integer value for manual zoom\nEg. value: 5 will fetch plots up to 5 km diagonally from site in both directions.", 
             "Neighbour list (comma separated), if blank first tier neighbours will be considered", 
             "Carrier List(comma separated)", ""]
        ]
        for site in lte_sites:
            earfcns = sorted(set(
                self.get_column_value(record, self.mappings["LTE"]["EARFCNDL"], "LTE")
                for tech, record in st.session_state.matched_records
                if tech == "LTE" and self.get_column_value(record, self.mappings["LTE"]["Site"], "LTE") == site
                and self.get_column_value(record, self.mappings["LTE"]["EARFCNDL"], "LTE")
            ))
            lte_data.append([site, "0", "", ",".join(earfcns), ""])
        ws_lte = wb.create_sheet("LTE")
        for row in lte_data:
            ws_lte.append(row)
        # NR Sheet
        nr_data = [
            ["Type", "Value", "Remarks", "Buffer", "", ""],
            ["projectName", st.session_state.project_name, "", "0.01", "", ""],
            ["startTime", datetime.now().strftime("%Y-%m-%d %H:%M:%S"), "Use exact format. Add \" ' \" before", "Default keep it 0.01. Can increase it to accommodate TA Plot", "", ""],
            ["endTime", datetime.now().strftime("%Y-%m-%d %H:%M:%S"), "Use exact format. Add \" ' \" before", "", "", ""],
            ["Site Name List", "ZOOM (Default = 0 for automatic zoom, enter integer value for manual zoom\nEg. value: