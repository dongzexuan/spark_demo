import lasio
from config import log_splice_config as ls_config


def spliced_las_writer(CORRECTED_LAS_NAME, single_well):

    ##############################################################
    # INSTANTIATE A NEW .LAS FILE OBJECT
    ##############################################################
    las = lasio.LASFile()

    ##############################################################
    # ADD CUSTOM COMMENTS...
    # TODO: The interpolation and printing of custom comments / labels does not quite work yet.
    ##############################################################
    las.sections.update({
        "Comments_1": "",
        "Comments_2": "",
        "Comments_3": "",
        "Comments_4": "",
        "Comments_5": ""})
    key_order = (
        "Comments_1",
        "Version",
        "Comments_2",
        "Well",
        "Comments_3",
        "Curves",
        "Comments_4",
        "Parameter",
        "Comments_5",
        "Other")
    las.sections = dict((k, las.sections[k]) for k in key_order)
    # print(las.sections.keys())

    # Comments_1
    line1 = f"" + "\n"
    line2 = f"# LAS WORX (tm) v17.01.12" + "\n"
    line3 = f"# Original File Owner: ANADARKO" + "\n"
    line4 = f"" + "\n"
    # ~Version ---------------------------------------------------

    # Comments_2
    line5 = f"" + "\n"
    line6 = f"# MNEM UNIT        VALUE/NAME              DESCRIPTION    " + "\n"
    line7 = f"# ---- ---- -------------------------   ------------------" + "\n"
    line8 = f"" + "\n"
    # ~Well ------------------------------------------------------

    # Comments_3
    line9 = f"" + "\n"
    line10 = f"# MNEM UNIT API CODES               DESCRIPTION          " + "\n"
    line11 = f"# ---- ---- ---------   ---------------------------------" + "\n"
    line12 = f"" + "\n"
    # ~Curves ----------------------------------------------------

    # Comments_4
    line13 = f"" + "\n"
    line14 = f"# MNEM UNIT       VALUE/NAME              DESCRIPTION    " + "\n"
    line15 = f"# ---- ---- -----------------------   -------------------" + "\n"
    line16 = f"" + "\n"

    # Comments_5
    # Headers / labels for the curve data should go here.

    ##############################################################
    # SET ALL HEADERS AND DATA
    ##############################################################
    las.sections["Comments_1"] = line1 + line2 + line3 + line4
    # ~Version ---------------------------------------------------
    las.version["VERS"] = lasio.HeaderItem("VERS", value="2.0")
    las.version["WRAP"] = lasio.HeaderItem("WRAP", value="NO")

    las.sections["Comments_2"] = line5 + line6 + line7 + line8
    # ~Well ------------------------------------------------------

    las.well["STRT"] = lasio.HeaderItem(
        mnemonic="STRT", value=single_well.top_depth, descr="START DEPTH")
    las.well["STOP"] = lasio.HeaderItem(
        mnemonic="STOP", value=single_well.bottom_depth, descr="STOP DEPTH")
    las.well["STEP"] = lasio.HeaderItem(
        mnemonic="STEP", value=single_well.step, descr="STEP")

    las.well["CNTY"] = lasio.HeaderItem(
        mnemonic="CNTY", value=single_well.county, descr="COUNTY")
    las.well["SRVC"] = lasio.HeaderItem(
        mnemonic="SRVC",
        value=single_well.logging_contractor,
        descr="SERVICE COMPANY")
    las.well["UWI"] = lasio.HeaderItem(
        mnemonic="UWI", value=single_well.uwi, descr="UNIQUE WELL ID")
    las.well["WELL"] = lasio.HeaderItem(
        mnemonic="WELL", value=single_well.wellname, descr="WELL NAME")

    # Adding add'l headers via attribute dot method will not work; must use
    # item-style access...
    las.well["LAT"] = lasio.HeaderItem(
        mnemonic="LAT", value=single_well.lat, descr="LATITUDE")
    las.well["LON"] = lasio.HeaderItem(
        mnemonic="LON", value=single_well.lon, descr="LONGITUDE")

    # Adding header for NULL values
    las.well["NULL"] = lasio.HeaderItem(
        mnemonic="NULL", value=ls_config.MISSING, descr="NULL VALUE")

    las.sections["Comments_3"] = line9 + line10 + line11 + line12
    # ~Curves ----------------------------------------------------
    # Curve description
    single_well.data.columns = single_well.data.columns.map(str.upper)
    col_names = single_well.data.columns.tolist()
    las.curves["DEPT"] = lasio.HeaderItem(mnemonic="DEPT", descr="Depth")
    for col_name in col_names:
        if col_name in ls_config.CURVE_DESC.keys():
            las.curves[col_name] = lasio.HeaderItem(
                mnemonic=col_name, descr=ls_config.CURVE_DESC[col_name])
        else:
            las.curves[col_name] = lasio.HeaderItem(
                mnemonic=col_name, descr="")

    las.sections["Comments_4"] = line13 + line14 + line15 + line16
    # ~Params ----------------------------------------------------

    # las.params['BHT'] = lasio.HeaderItem(
    #     mnemonic='BHT',
    #     value=single_well.bottom_hole_pressure,
    #     descr='BOTTOM HOLE TEMPERATURE')
    # las.params['BS'] = lasio.HeaderItem(
    #     mnemonic='BS',
    #     value=single_well.bit_size,
    #     descr='BIT SIZE')
    las.params["LCNM"] = lasio.HeaderItem(
        mnemonic="LCNM",
        value=single_well.logging_contractor,
        descr="LOGGING CONTRACTOR")
    # las.params['RMF'] = lasio.HeaderItem(
    #     mnemonic='RMF',
    #     value= single_well.mud_filtrate_resistivity,
    #     descr='MUD FILTRATE RESISTIVITY')
    las.params["DFD"] = lasio.HeaderItem(
        mnemonic="DFD",
        value=single_well.mud_density,
        descr="DRILL FLUID DENSITY")
    las.params["MRT"] = lasio.HeaderItem(
        mnemonic="MRT", value=single_well.max_rec_temp, descr="MAX REC TEMP")
    las.params["RMS"] = lasio.HeaderItem(
        mnemonic="RMS",
        value=single_well.mud_resistivity,
        descr="MUD RESISTIVITY")
    las.params["MST"] = lasio.HeaderItem(
        mnemonic="MST", value=single_well.mud_temp, descr="MUD TEMP")
    las.params["MFST"] = lasio.HeaderItem(
        mnemonic="MFST",
        value=single_well.mud_density,
        descr="DRILL FLUID DENSITY")  # mud_density TWICE??

    names = las.curves.keys()

    # ~Other -----------------------------------------------------
    # ~ASCII -----------------------------------------------------
    # single_well.data = single_well.data[names].set_index('DEPT')
    las.set_data(single_well.data, names=names)

    ##############################################################
    # WRITE ALL HEADERS AND DATA
    ##############################################################
    las.write(CORRECTED_LAS_NAME, version=2, fmt="%.3f")


