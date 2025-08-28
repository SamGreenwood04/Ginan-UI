"""
===============================================================================
CDDIS_Handler Header
===============================================================================
------------------------------------------------------------------------------
Usefull stuff for accessing 
------------------------------------------------------------------------------
#checks if the user can fetch the cddis files
validate_netrc(machine="urs.earthdata.nasa.gov") -> tuple[bool, str]:
# will return (true,"") when everything is good
# will return (false,"some error") when everything is not good 
------------------------------------------------------------------------------
Usefull stuff for accessing cddis data
------------------------------------------------------------------------------
from app.cddis_handler import CDDIS_Handler

#auto_populate values if found if none are found then -> (none, none)
project_type_optimal, solution_type_optimal = my_cddis.get_optimal_project_solution_tuple("COD") 

# returns list of valid analysis_centers that user can input
valid_ac = my_cddis.get_list_of_valid_analysis_centers()

# returns list of valid project_types for given analysis_centers
project_types = my_cddis.get_list_of_valid_project_types("COD") 

# returns list of valid solution_types for given analysis_centers
solution_types = my_cddis.get_list_of_valid_solution_types("COD")

# validate user input is valid where inputs("analysis_center","project_types","solution_type")
is_valid = my_cddis.is_valid_project_solution_tuple("COD","MGX","FIN")

"""

import re
from datetime import datetime, timedelta
import pandas as pd
from collections import defaultdict
from pathlib import Path
from dotenv import load_dotenv
import numpy as np
from app.utils.gn_functions import GPSDate
import requests
from bs4 import BeautifulSoup
import netrc
import platform

BASE_URL = "https://cddis.nasa.gov/archive"

def validate_netrc(machine="urs.earthdata.nasa.gov") -> tuple[bool, str]:
    """
    runs checks on the .netrc file to make sure it's valid

    :param machine: The target credentials to use defaulted to urs.earthdata.nasa.gov
    :returns (bool,str): If returns true then string will be empty. If false string will contain error message 
    """
    if platform.system() == "Windows":
        netrc_path = Path.home() / "_netrc"
    else: # will assume linux 
        netrc_path = Path.home() / ".netrc"

    if not netrc_path.exists():
        #(f".netrc wasn't found at {netrc_path}")
        return False, (f".netrc wasn't found at {netrc_path}")
    try:
        credentials = netrc.netrc(netrc_path).authenticators(machine)
        if credentials is None:
            #print(f"EarthData registration: https://urs.earthdata.nasa.gov/users/new")
            #print(f"Instructions for creating .netrc file: https://cddis.nasa.gov/Data_and_Derived_Products/CreateNetrcFile.html")
            return False, f"Incomplete credentials for '{machine}' in .netrc"
        login, _, password = credentials
        if not login or not password:
            #print()
            return False, f"Incomplete credentials for '{machine}' in .netrc"
        return True, ""

    except (netrc.NetrcParseError, FileNotFoundError) as e:
        return False, f"Error parsing .netrc: {e}"
    
# note on merge conflict change GPSDate -> int
def retrieve_all_cddis_types(gps_week: int) -> list[str]:
    """
    Retrieve CDDIS file list for a specific GPS week. Using Html get
    
    :param gps_week: int value that represent the GPS week e.g 2052

    :return files: Warning unsanatised this will return all files includeing files in bad format 
    """

    url = f"https://cddis.nasa.gov/archive/gnss/products/{gps_week}/"
    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
    except requests.RequestException as e:
        print(f"Failed to fetch files for GPS week {gps_week}: {e}")
        return []
    
    # Parse the HTML links for file names
    soup = BeautifulSoup(response.text, 'html.parser')
    
    files = [a['href'] for a in soup.find_all('a',class_="archiveItemText", href=True) if not a['href'].endswith('/')]
    return files

# Note create_cddis_file not currently used anywhere
# if we are going to be do caching  
# i'll shift the input to be an list of ints
# and change the output file into a yaml
def create_cddis_file(filepath: Path, reference_start: GPSDate) -> None:
    """
    Create a file named "CDDIS.list" with CDDIS data types for a given reference start time.
    """
    data = retrieve_all_cddis_types(reference_start)
    cddis_file_path = filepath / "CDDIS.list"

    with open(cddis_file_path, "w") as f:
        for d in data:
            try:
                time = datetime.strptime(d.split("_")[1], "%Y%j%H%M")
                f.write(f"{d} {time}\n")
            except (IndexError, ValueError):
                continue

def retrieve_all_cddis_types(reference_start:int ) -> list[str]:
    """
    Retrieve all CDDIS data types for a given GPS Week.
    """
    load_dotenv(Path(__file__).parent.parent / "utils" / "cddis.env")  # Adjust path

    ftp_tls = FTP_TLS(host="gdc.cddis.eosdis.nasa.gov", user="anonymous", passwd=os.getenv("EMAIL"), timeout=60)
    ftp_tls.prot_p()
    files = None
    try:
        ftp_tls.cwd(f"gnss/products/{reference_start}")
        files = ftp_tls.nlst()
    except ftplib.all_errors as e:
        print("Error getting file list", e)
    finally:
        ftp_tls.quit()
    return files or []


def create_cddis_file(filepath: Path, reference_start: GPSDate) -> None:
    """
    Create a file named "CDDIS.list" with CDDIS data types for a given reference start time.
    """
    data = retrieve_all_cddis_types(reference_start)
    cddis_file_path = filepath / "CDDIS.list"

    with open(cddis_file_path, "w") as f:
        for d in data:
            try:
                time = datetime.strptime(d.split("_")[1], "%Y%j%H%M")
                f.write(f"{d} {time}\n")
            except (IndexError, ValueError):
                continue

class CDDIS_Handler ():
    def __init__(self,date_time_start_str:str, date_time_end_str:str,target_files = ["CLK","BIA","SP3"]):    
        """
        CDDIS object constructor. Requires CDDIS input and date_time input inorder to access getters 
        :param date_time_end_str: YYYY-MM-DD_HH:mm:ss Path to the CDDIS.list ( on init required for query) e.g 2025-05-01_00:00:00
        :returns: cddis handeler object 
        """      
        self.df             = None     # pd.dataframe of cddis input
        self.valid_products = None     # pd.dataframe holding valids products
        self.time_end       = None     # user end time bound
        self.time_start     = None     # user end time bound
        self.target_files   = target_files

        # Note can shift over to YYYY-dddHHmm format if needed through datetime.strptime(date_time,"%Y%j%H%M") 
        self.time_end = self.__str_to_datetime(date_time_end_str)
        self.time_start = self.__str_to_datetime(date_time_start_str)       
        #self.__download_cddis(self.time_start,self.time_end) #potential improvment instaed of writing out into file write into mem buff
        self.__get_cddis_list(self.time_start,self.time_end)
        self.__set_valid_products_df()

    def __get_cddis_list(self,start_date_time:datetime,end_date_time:datetime):
        """
        PRIVATE METHOD

        gets the list of cddis values as a list populated class data frame

        :param start_date_time: datetime object
        :param end_date_time: datetime object 
        """
        gps_start_week = GPSDate(np.datetime64(start_date_time))
        gps_end_week   = GPSDate(np.datetime64(end_date_time))
        # data is stored in weekly format i.e /gps_week(num)/data
        gps_weeks      = list(range(int(gps_start_week.gpswk), 
            int(gps_end_week.gpswk) + 1))
        cddis_list = []
        ########
        # potential multi thread area
        # make a bunch of seperate calls then 
        try:
            for gps_week in gps_weeks:
               cddis_list += retrieve_all_cddis_types(gps_week)
        except:
            raise TimeoutError("CDDIS download timedout")
        # semaphor lock until all return 
        
        self.__df_parse_cddis_str_array(cddis_list)

    def __str_to_datetime(self, date_time_str):
        """
        PRIVATE METHOD

        :param date_time_str: YYYY-MM-DD_HH:mm:ss
        :returns datetime: datetime.strptime()
        """
        # Note can shift over to YYYY-dddHHmm format if needed through datetime.strptime(date_time,"%Y%j%H%M") 
        try: 
            return datetime.strptime(date_time_str, "%Y-%m-%d_%H:%M:%S")
        except ValueError:             
            raise ValueError("Invalid datetime format. Use YYYY-MM-DDTHH:MM (e.g. 2025-05-01_00:00:00)")

    def __df_parse_cddis_str_array(self, cddis_str_array:list[str]):
        """
        PRIVATE METHOD


        Generates an pd data frame. 
        
        :param cddis_str_array: input array of line str containg following 
        pattern COD0MGXFIN_20250960000_01D_01D_OSB 
        :returns: None (updates the objects data frame) 
        """  
        # INPUT 
        # COD0MGXFIN_20250960000_01D_01D_OSB.BIA.gz 2025:04:17 10:38:56    63.19KB
        # Most important will be 
        # COD0MGXFIN_20250960000
        # the trailing will be mostly ignored.
        # re.compile(r'^([A-Z0-9]{3})[0-9]([A-Z]{3})([A-Z]{3})_(\d{11})')
        # regex to extract from list 
        # analysis_center    3  char
        # 0                  1  padding   
        # project_type       3  char
        # solution_type      3  char
        # date yyyydddhhmm   11 char 
        #
        #
        # <*> . matches 
        #pattern = re.compile(r'^([A-Z0-9]{3})[0-9]([A-Z]{3})([A-Z]{3})_(\d{11})_.{3}_.{3}_.{3}.([A-Z]{3})')
        pattern = re.compile(r'^([A-Z0-9]{3})[0-9]([A-Z]{3})([A-Z]{3})_(\d{11})_([0-9]{2})._([0-9]{2})._.{3}.([A-Z0-9]{3})')
        
        parsed_data = []

        for line in cddis_str_array:

            parts = line.strip().split()        
            if not parts:
                # malformed input
                continue 
            filename = parts[0]

            match = pattern.match(filename)
            if match:
                    analysis_center, project_type, solution_type, end_validity_str,duration,sample_rate, file_type = match.groups()
                    try:
                        end_datetime = datetime.strptime(end_validity_str, "%Y%j%H%M") #yyyydddhhmm
                        duration = timedelta(int(duration))
                        parsed_data.append({
                            "analysis_center": analysis_center,
                            "project_type": project_type,
                            "solution_type": solution_type,
                            "end_validity": end_datetime,
                            "file_type": file_type,
                            "duration": duration,
                            "filename": filename
                        })
                    except ValueError:                        
                        continue
        self.df = pd.DataFrame(parsed_data)

    def __parse_product_list_file(self, file_path:str):
        """
        PRIVATE METHOD

        Generates an pd data frame. 
        
        :param file_path: Path to the CDDIS.list
        :returns: None (updates the objects cddis dataframe)
        """  
        try: 
            with open(file_path, 'r') as file:
                lines = file.readlines()
        except (FileNotFoundError, IOError):
            # ERROR HANDLING 
            #print("Wrong file or file path")
            raise Exception("cddis_handler __parse_product_list_file: Wrong file or file path")

        self.__df_parse_cddis_str_array(lines)
    
    def __set_valid_products_df(self):
        """
        PRIVATE METHOD

        Sets the valid products data frame if valid date time and cddis data frame is available in object. 
        Is called after some value has been set in object. Will then update valid_products data frame 
        """
        if(self.time_end != None and 
           not isinstance(self.df, type(None))
           ):
            self.valid_products = self.get_valid_products_by_datetime(date_time_start=self.time_start,date_time_end=self.time_end,target_files=self.target_files)

    def __analysis_center_isin(self,analysis_center):
        if(analysis_center in self.get_list_of_valid_analysis_centers()):
            return True
        raise ValueError(f"{analysis_center} is not a valid anaylsis center")
        

    def set_date_time(self,date_time_start_str:str,date_time_end_str:str):
        """
        method will set cddis internals. based on provided date time 
        If the object has been given an date time then it will also populate 
        the objects valid products data frame.  

        :param date_time_end_str: YYYY-MM-DDTHH:MM (e.g. 2025-04-14T01:30) 

        :returns: None (updates object internals)
        """
        
        # should really create a dedicated helper function for this 
        self.time_end = self.__str_to_datetime(date_time_start_str)
        self.time_end = self.__str_to_datetime(date_time_end_str)
        self.__get_cddis_list(self.time_start,self.time_end)
        self.__set_valid_products_df()
        
    def get_valid_products_by_datetime(self, date_time_start:datetime = None, date_time_end:datetime = None, target_files:list[str] = None):
        """
        gets a dataframe of valid products by datetime based on input date time and object CDDIS list

        :param date_time_start: datetime.strptime(date_time_str, "%Y-%m-%dT%H:%M")
        :param date_time_end: datetime.strptime(date_time_str, "%Y-%m-%dT%H:%M")
        :returns valid_products: pd.df of valid products in ({"analysis_center": [], "analysis_center": [(project_type,solution_type)]}) if no valids then return empty df
        """

        # upper boundary prune         
        products = self.df[(self.df["end_validity"]+self.df["duration"] >= date_time_end)]
        #products = self.df
        #print(products["file_type"].unique())
        #print(products["files_types"])
        
        product_tuples = defaultdict(set)
        for _, row in products.iterrows():
            product_tuples[
                row["analysis_center"]].add(
                    (row["project_type"], row["solution_type"])
                    )

        
        product_tuples = pd.DataFrame([
            {"analysis_center": k, "available_types": sorted(list(v))}
            for k, v in product_tuples.items()
        ])
        # lower bound checks
        # validation downwards

        products = self.df[(self.df["end_validity"] <= date_time_start)]

        valid_products = defaultdict(set)
        
        for _, row in product_tuples.iterrows():
            for project_type,solution_type in row["available_types"]:
                
                selected_rows_valid = True

                selected_rows = products.loc[(products["analysis_center"] == row["analysis_center"]) & 
                           (products['project_type'] == project_type) & 
                           (products['solution_type'] == solution_type)]
    
                for time in selected_rows["end_validity"].unique():
                    # getting products part of set
                    selected_product_set = selected_rows.loc[(selected_rows["end_validity"] == time)]
                    files_types = selected_product_set["file_type"].unique()
                    # check for target files 
                    for target_file in target_files:
                        if not(target_file in files_types):
                            selected_rows_valid = False
                            break

                    if(not selected_rows_valid):
                        break

                # project anaylsis_center project_type solution_type
                # typle has missing file/s
                if (selected_rows_valid):
                    valid_products[
                        row["analysis_center"]].add(
                            (project_type, solution_type)
                            )

        valid_products = pd.DataFrame([
            {"analysis_center": k, "available_types": sorted(list(v))}
            for k, v in valid_products.items()
        ])
               
        if(valid_products.empty):
            raise RuntimeError("no valid product tuple found for input datetime") 

        return valid_products
    
    def get_list_of_valid_analysis_centers(self) -> list[str]:
        """
        outputs a list str of valid analysis centers

        :param: None
        :return list[str]: list string of valid analysis centers
        """
        return self.valid_products["analysis_center"].unique()

    def get_df_of_valid_types_tuples(self,analysis_center:str):
        """
        outputs a pandas data frame of valid tuple pairings of project-type solution-type 
        for given analysis_center  

        :param analysis_center: target analysis center  
        :return: pd.df in form of ({project-type: str,solution-type str) 
        """
        self.__analysis_center_isin(analysis_center)
        #long boi
        tuple_array = self.valid_products.loc[self.valid_products["analysis_center"]==analysis_center].iloc[0]["available_types"]
        
        df = pd.DataFrame(tuple_array, columns=['project-type','solution-type'])
        return df
    
    def get_list_of_valid_project_types(self,analysis_center:str) -> list[str]:
        """
        outputs a list string of valid_project_types

        :param analysis_center: target analysis center  
        :return: list string of valid_project_types
        """
        self.__analysis_center_isin(analysis_center)
        df = self.get_df_of_valid_types_tuples(analysis_center)
        return df['project-type'].unique().tolist() 
        
    def get_list_of_valid_solution_types(self,analysis_center:str)-> list[str]:
        """
        outputs a list string of valid_solution_types

        :param analysis_center: target analysis center  
        :return: list string of valid_solution_types
        """
        self.__analysis_center_isin(analysis_center)
        df = self.get_df_of_valid_types_tuples(analysis_center)
        return df['solution-type'].unique().tolist() 
    
    def is_valid_project_solution_tuple(self, analysis_center:str, project_type:str, solution_type:str):
        """
        Verification of sanatised user input of analysis_center project_type solution_type
 
        :param analysis_center: user input analysis_center
        :param project_type:    user input project_type
        :param solution_type:   user input solution_type
        
        :return: bool valid if user input is valid
        """
        self.__analysis_center_isin(analysis_center)
        df_valid_tuples = self.get_df_of_valid_types_tuples(analysis_center)
            
        is_empty = df_valid_tuples[(df_valid_tuples["project-type"] == project_type) & 
                        (df_valid_tuples['solution-type'] == solution_type)].empty
        
        #if empty then invalid  if not then valid
        return not is_empty

    def get_optimal_project_solution_tuple(self,analysis_center:str,satellite_constellations = None) -> tuple[str, str]:
                
        """
        From available valids from selected anaylis_center
        solution_type_priorities FIN -> RAP -> ULT  
        Future proof satellite_constellations will help determine the project_type 

        :param analysis_center: target analysis center
        :param satellite_constellations: NOT IMPLEMENTED
        :return: on success (project_type,solution_type) else (None,None)
        """
        # place holder for future proofing for yaml read in 
        # logic for determining project_type priorites
        # probably store some sort of tuple set in resourses folder
        project_type_priorities = None
        if(not satellite_constellations):
            project_type_priorities = ["MGX"] # "OPS","DEM"
        else:
            raise NotImplementedError("satellite_constellations used in  project_type_priorities not implemented yet")
        
        solution_type_priorities = ["FIN","RAP","ULT"]
        
    
        priority_mapping = None

        df_valid_tuples = self.get_df_of_valid_types_tuples(analysis_center)

        for project_type_priority in project_type_priorities:
            valid_solution_types = df_valid_tuples[(df_valid_tuples["project-type"] == project_type_priority)]['solution-type'].to_list()
            for solution_type_priority in solution_type_priorities:
                if(solution_type_priority in valid_solution_types):
                    return project_type_priority,solution_type_priority
                           
        return None,None

if __name__ == "__main__":
    #my_cddis = CDDIS_Handler(date_time_start_str="2024-04-14_01:30:00",date_time_end_str="2024-04-14_01:30:00")
    #my_cddis = CDDIS_Handler(date_time_start_str="2025-07-05_00:00:00",date_time_end_str="2025-07-05_23:59:30")
    #my_cddis = CDDIS_Handler(date_time_start_str="2025-07-05_00:00:00",date_time_end_str="2025-07-05_00:00:00")
    
    #my_cddis = cddis_handler(cddis_file_path="app/resources/cddis_temp/CDDIS.list",date_time_end_str="2024-04-14T01:30")
    # note that cddis.env setup in utils see download_products.py

    #my_cddis = CDDIS_Handler(date_time_start_str="2019-07-18_00:00:00", date_time_end_str="2019-07-18_23:59:30") # will filter for target files ["CLK","BIA","SP3"]
    my_cddis = CDDIS_Handler(date_time_start_str="2025-07-05_00:00:00",date_time_end_str="2025-07-05_00:00:00")
    
    print(my_cddis.df)
    print(my_cddis.valid_products)
    print(my_cddis.get_list_of_valid_analysis_centers())

    print(my_cddis.get_df_of_valid_types_tuples("COD"))
    print(my_cddis.get_list_of_valid_project_types("COD"))
    print(my_cddis.get_list_of_valid_solution_types("COD"))
    print(my_cddis.is_valid_project_solution_tuple("COD","MGX","FIN"))
    print(my_cddis.get_optimal_project_solution_tuple("COD"))
    print(my_cddis.get_optimal_project_solution_tuple("EMR"))
    print(my_cddis.time_end)
    print(my_cddis.df)
    print(my_cddis.valid_products)