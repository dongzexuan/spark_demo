import pyspark as ps
import warnings
import sys
import re
import lasio
import os
import pandas as pd
import copy
from config import log_splice_config as ls_config
from utils.las_writer import spliced_las_writer
inputs_pattern = 'data\*'
outputs_prefix = 'outputs\dict'
input_dir = 'test_data'
output_dir = 'outputs'
uwi_re = re.compile("[0-9]{14,}")


class well_log(object):
    """
    Initializes, gets, and sets all properties for the well log object.
    """

    def __init__(self):
        """
        Initializes an empty well__log object.

        :param uwi: Well UWI
        :param label: Well label
        :param top_depth: Top depth of the well log
        :param bottom_depth: Bottom depth of the well log
        :param step: Depth difference between measurements
        :param data: Pandas data frame with all measurements
        :param x: Well X coordinate
        :param y: Well Y corrdinate
        :param gamma: Gamma ray log
        :param resistivity: Resistivity log
        :param log_resistivity: Log10 of resistivity log
        """

        self.data = None
        self.corrected_data = None
        self.file_name = None
        self.uwi = ls_config.MISSING
        self.wellname = None
        self.lat = ls_config.MISSING
        self.lon = ls_config.MISSING

        self.data_pre_processed = False
        self.data_has_bhf = False

        self.top_depth = ls_config.MISSING
        self.bottom_depth = ls_config.MISSING
        self.step = ls_config.MISSING
        self.county = None
        self.date = None

        # self.__log_type = log_type.empty
        self.__log_type = 0
        self.__original_curves = []

        self.__tops = dict()
        self.__clean_tops = dict()
        self.__log_types_by_zone = dict()
        self.close_wells = []

        self.bottom_hole_pressure = 0
        self.bit_size = 0
        self.logging_contractor = None
        self.max_rec_temp = 0
        self.mud_density = 0
        self.mud_resistivity = 0
        self.mud_temp = 0
        self.mud_filtrate_resistivity = 0
        self.mid_filtrate_temp = 0

        self.original_rdeep = None
        self.rdeep_original = None
        # UNUSED SO FAR
        self.gamma = None
        self.log_resistivity = None
        self.conductivity = None

    @classmethod
    def init_from_lasio(
            cls,
            lasio_data,
            lasio_well_parameters,
            lasio_file_parameters):
        """
        Default values initialized by the well log reader.

        :param lasio_data: raw well log data.
        :param lasio_well_parameters: keys and values from log data.
        :param lasio_file_parameters: not implemented
        """
        well = cls()

        # Assign data
        well.data = lasio_data

        used_well_params = {
            "STRT": ls_config.MISSING,
            "STOP": ls_config.MISSING,
            "STEP": 0,
            "UWI": None,
            "CNTY": None,
            "DATE": None,
            "LAT": ls_config.MISSING,
            "LON": ls_config.MISSING,
            "WELL": None}

        used_file_params = {
            "BHT": ls_config.MISSING,
            "BS": ls_config.MISSING,
            "LCNM": None,
            "MRT": ls_config.MISSING,
            "DFD": ls_config.MISSING,
            "RMS": ls_config.MISSING,
            "MST": ls_config.MISSING,
            "RMFS": ls_config.MISSING,
            "MFST": ls_config.MISSING}

        # Get all the parameters
        for p in used_well_params:
            if p in lasio_well_parameters.keys():
                used_well_params[p] = lasio_well_parameters[p]["value"]

        # Assign to class variables
        # if curves.rdeep_original.name in well.data.columns:
        #    well.rdeep_original = well.data['rdeep_original']
        #    well.data.drop('rdeep_original', axis=1, inplace=True)

        well.top_depth = float(used_well_params["STRT"])
        well.bottom_depth = float(used_well_params["STOP"])
        well.step = float(used_well_params["STEP"])
        well.uwi = str(used_well_params["UWI"])
        well.county = used_well_params["CNTY"]
        well.date = used_well_params["DATE"]
        well.lat = float(used_well_params["LAT"])
        well.lon = float(used_well_params["LON"])
        well.wellname = used_well_params["WELL"]
        well.__original_curves = well.data.columns

        well.bottom_hole_pressure = float(used_file_params["BHT"])
        well.bit_size = float(used_file_params["BS"])
        well.logging_contractor = used_file_params["LCNM"]
        well.max_rec_temp = float(used_file_params["MRT"])
        well.mud_density = float(used_file_params["DFD"])
        well.mud_resistivity = float(used_file_params["RMS"])
        well.mud_temp = float(used_file_params["MST"])
        well.mud_filtrate_resistivity = float(used_file_params["RMFS"])
        well.mid_filtrate_temp = float(used_file_params["MFST"])

        return well


def get_las_uwi(element):
    k = re.findall("[0-9]{14,}", element)
    if len(k) > 0:
        return (k[0], element)
    else:
        try:
            las = lasio.read(
                os.path.join(input_dir, element),
                null_policy="none",
                ignore_data=True)
            k = str(las.well["UWI"]["value"])
            return (k, element)

            # return [(k, element)]
        except:
            print("Unexpected error:", sys.exc_info()[0])


def merge_files(las_path_list):
    # merge las files according to UWI
    i = 1
    well_data = pd.DataFrame()
    well_data_merged = pd.DataFrame()
    well_parameters = ''
    file_parameters = ''
    for las_path in las_path_list:
        filename = os.path.join(input_dir, las_path)
        try:
            # GCP input
            las = lasio.read(filename, null_policy="none")
            well_data = las.df()
            if well_data.empty:
                continue
            # remove redundant rows with duplicated index (depth)
            well_data = well_data[~well_data.index.duplicated(
                keep="first")]
            col_names = well_data.columns.tolist()
            if len(os.path.commonprefix(col_names)) > 1:
                las_prefix = os.path.commonprefix(col_names)
            else:
                las_prefix = ""
            prefix_remove = re.compile("^" + las_prefix)
            # removing prefixes
            # no_prefix_names = [prefix_remove.sub('', name) for name in col_names]
            no_prefix_names = [prefix_remove.sub("", name) + ":"
                               + str(i) for name in col_names]

            # rename columns
            well_data.columns = no_prefix_names
            # Replace NaN with ls_config.MISSING
            # Assumes NULL header is populated if NULL values are present
            # Additionally, NULL value should be a float (not a string)
            if "NULL" in las.well:
                well_data[well_data ==
                          las.well.NULL.value] = -999.0
            well_data.fillna(-999.0, inplace=True)
            well_parameters = las.well
            file_parameters = las.params
            if las_path == las_path_list[0]:
                well_data_merged = copy.deepcopy(well_data)
            else:
                if not well_data_merged.empty:
                    well_data_merged = pd.concat(
                        [well_data_merged, well_data],
                        axis=1,
                        join="outer",
                        sort=False)
                else:
                    well_data_merged = copy.deepcopy(well_data)
                well_data_merged.sort_index(inplace=True)
            well_data_merged.fillna(-999.0, inplace=True)

            i = i + 1
        except:
            print("Unexpected error:", sys.exc_info()[0])
            continue
    return well_data_merged, well_parameters, file_parameters


def output_files(element):
    las_path_list = element[1]
    if len(las_path_list) < 2:
        print('no need to merge')
    else:
        well_data_merged, well_parameters, file_parameters = merge_files(las_path_list)
        merged_name = 'Merged_' + element[0] + '.las'
        w = well_log.init_from_lasio(
            well_data_merged, well_parameters, file_parameters)
        
        spliced_las_writer(os.path.join(output_dir, merged_name), w)

        return merged_name


def main():
    try:
        # create SparkContext on all CPUs available: in my case I have 2 CPUs on my pc
        sc = ps.SparkContext('local[2]')
        print("Just created a SparkContext")
    except ValueError:
        warnings.warn("SparkContext already exists in this scope")
    file_list = [f for f in os.listdir(input_dir) if f.lower().endswith('.las')]

    # create Spark pipeline to process las merging in parallel
    files = sc.parallelize(file_list)  # distribute files among nodes

    # create UWI dictionary, merge files according to dictionary
    uwi = files.map(get_las_uwi)\
        .groupByKey()\
        .mapValues(list)\
        .map(output_files)

    print('Number of merged files: ' + str(uwi.count()))


if __name__ == '__main__':
    main()