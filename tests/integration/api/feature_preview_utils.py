"""
Feature preview utils
"""
import pandas as pd


def convert_preview_param_dict_to_feature_preview_resp(input_dict):
    """
    Helper function to convert preview param dict to feature preview response
    """
    output_dict = input_dict
    output_dict["POINT_IN_TIME"] = pd.Timestamp(input_dict["POINT_IN_TIME"])
    return output_dict
