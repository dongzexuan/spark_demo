PROJECT_ID = "aaet-geoscience-dev"
# The tmp folder is for lasio I/O purposes
DATA_PATH = "/home/airflow/gcs/data/tmp"

# Credential JSON key for accessing other projects
# CREDENTIALS_JSON = "gs://aaet_zexuan/flow/keys/composer_las_merge.json"
CREDENTIALS_JSON = "keys/composer_las_merge.json"

# Bucket name for merged las files and spliced las files
BUCKET_LAS_MERGE = "las_merged"
BUCKET_LAS_SPLICE = "us-central1-lithos-dev-94beb3d4-bucket"

# las_splice.py output to the composer data folder, as input of logqc
COMPOSER_FOLDER = "data/logqc_landing"
TMP_FOLDER = "data/tmp"
# for GCP web UI and Big Query Job Status Report
BUCKET_JOB = "log_splice_tool_jobs"
BIGQUERY_DATASET_ID = "urc_jobs"
BIGQUERY_TABLE_ID = "jobs"

# Workflow type
tpt_workflow_type = "tpt"
logsplice_workflow_type = "logsplice"
logqc_workflow_type = "logqc"
geomech_workflow_type = "geomech"

# Number of processors for las_merge_MP (multiprocessing).
N_PROCESSORS = 16

# The window size for moving average, e.g. 11 means the window covers a
# point and 5 adjacent points on both sides
MOVING_AVG_WINDOW_SIZE = 11

# Default value for missing data, usually it is either -999.25 or -999.0
MISSING = -999.0

# COL_DICT: a dictionary of aliased curve names for log splicing. keys correspond to measurements
# (e.g., 'density', 'gamma', 'resistivity', etc.),
# and each value is a list of aliased column names that could potentially correspond
# to those measurements. Each key is the aliased curve name before splicing,
# each key's value is the standard curve name after splicing.
COL_DICT = {
    # Caliper
    "cal": ["CAL", "CALI", "CALX", "HCAL", "TGS_CALX", "RAW_CALX"],
    # Compressional Sonic Slowness
    "dtc": ["DT", "DT24", "DTC", 'TGS_DT', "TGS_DTC", "RAW_DT", "RAW_DTC"],
    # Deep Resistivity
    # 'rdeep' includes 'rdeep_ltrl' (laterolog), 'rdeep_indct' (induction), 'rdeep_unknown'.
    # A final 'rdeep' will be generated
    # with an additional 'rdeep_type' curve to denote the log type.
    "rdeep": ['ILT90', 'LLD', 'RDEEP', 'RES', 'RES_DEEP', 'AHT90', 'AT90', 'ILD', 'ILT90', 'LLD', 'ILO90', 'ILF90', 'LLMD'],
    # Density (Bulk)
    "rhob": ["DEN", "RHOB", "RHOZ", "ZDEN", "ZDNC", "TGS_RHOB", 'RAW_RHOB'],
    # Density (Correction)
    "drho": ["DRHO", "HDRA", "ZCOR"],
    # Gamma Ray
    "gr": ["APC_GR_NRM", "GAMM", "GR", "GR_R", "GRR", 'SGR', 'SGRR', 'CGR'],
    # Neutron Porosity
    "nphil": ["CNCF", "NEU", "NPOR", "NPHI", "NPHIL", "TNPH", 'TGS_NPHI', 'NPHI_LS', 'TNPH_LS', 'RAW_NPHI'],
    # Photoelectric effect
    "pe": ["PE", "PEF", "PEFZ", 'TGS_PE', 'RAW_PE'],
}

# LDD is laterolog
# The rest are inductions
# RDEEP, RES, RES_DEEP are of unknown origin
# __log_type_rdeep = [log_type_enum.induction,  #AHT90
#                     log_type_enum.induction,  #AT90
#                     log_type_enum.induction,  #ILD
#                     log_type_enum.induction,  #ILT90
#                     log_type_enum.laterolog,  #LLD
#                     log_type_enum.induction,  #M2R9
#                     log_type_enum.unknown,    #RDEEP
#                     log_type_enum.unknown,    #RES
#                     log_type_enum.unknown]    #RES_DEEP

RDEEP_TYPE_LIST = ["rdeep_ltrl", "rdeep_indct", "rdeep_unknown"]
RDEEP_TYPE_DICT = {"rdeep_ltrl": 1, "rdeep_indct": 2, "rdeep_unknown": 3}

# curve description dictionary
CURVE_DESC = {
    "DEPT": "Depth",
    "CAL": "Caliper",
    "DRHO": "Density Correction",
    "DTC": "Compressional Wave Slowness",
    "DTS": "Shear Wave Slowness",
    "GR": "Gamma Ray",
    "NPHI": "Neutron Porosity",
    "NPHIL": "Neutron Porosity",
    "PE": "Photoelectric Effect",
    "RDEEP": "Deep Resistivity",
    "RDEEP_LTRL": "Laterolog Resistivity",
    "RDEEP_INDCT": "Induction Resistivity",
    "RDEEP_UNKNOWN": "Unknown Resistivity (Laterolog or Induction)",
    "RDEEP_TYPE": "RDEEP Type 1:Laterolog 2:Induction 3:Unknown",
    "RHOB": "Bulk Density",
    "RUGOSITY": "Borehole Rugosity",
    "RUGOSITY_BHF": "Rugosity Bad Hole Flag",
    "DRHO_BHF": "Density Correction Bad Hole Flag",
    "DTC_BHF": "Sonic Bad Hole Flag",
    "GR_BHF": "Gamma Ray Bad Hole Flag",
    "NPHIL_BHF": "Neutron Bad Hole Flag",
    "RHOB_BHF": "Density Bad Hole Flag",
    "LOG_RDEEP_BHF": "Resistivity Bad Hole Flag",
    "PE_BHF": "PE Bad Hole Flag",
    "RHOB_MCF": "Density Corrected from Multiwell Flag",
    "RHOB_SYN": "Density Estimation from Ensemble of Learners",
    "NPHI_MCF": "Neutron Corrected from Multiwell Flag",
    "NPHI_SYN": "Neutron Estimation from Ensemble of Learners",
    "DTC_MCF": "Sonic Corrected from Multiwell Flag",
    "DTC_SYN": "Sonic Estimation from Ensemble of Learners",
    "PE_MCF": "PE Corrected from Multiwell Flag",
    "PE_SYN": "PE Estimation from Ensemble of Learners",
    "RHOB_NCF": "Density No Correction Flag",
    "RHOB_CORR": "Density Corrected",
    "NPHI_NCF": "Neutron No Correction Flag",
    "NPHI_CORR": "Neutron Corrected",
    "DTC_NCF": "Sonic No Correction Flag",
    "DTC_CORR": "Sonic Corrected",
    "PE_NCF": "PE No Correction Flag",
    "PE_CORR": "PE Corrected"
}
