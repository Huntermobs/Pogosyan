import os
import re
import pandas as pd


class Data_loading:
    """Class for data loading"""
    def __init__(self):
        self.final_frame = None
        self.temp_frame = None
        self.file_list = []
        self.date_list = []
        self.set_dates = None
        self.read_xlsx = None
        self.colunm_list = []

    def _create_lists(self, directory):
        """
            Finding of all .csv files in directory, if read_xlsx set True, .xlsx also add to list.
            If set_dates set True, also extract last date from filename
            Args:
                directory (str): path to directory with target files.

            Returns:
                None
        """
        if directory is not None:
            for f in os.scandir(directory):
                if f.is_file() and f.path.split('.')[-1].lower() == 'csv':
                    self.file_list.append(f.path)
                    if self.set_dates:
                        dates = re.findall(r"\d{4}-\d{2}-\d{2}", f.path)
                        if len(dates) > 1:
                            date = dates[1]
                        else:
                            date = dates
                        self.date_list.append(date)
                        # self.date_list.append(f.path[-15:-5])
                if self.read_xlsx and f.path.split('.')[-1].lower() == 'xlsx':
                    self.file_list.append(f.path)
        else:
            for f in os.scandir():
                if f.is_file() and f.path.split('.')[-1].lower() == 'csv':
                    self.file_list.append(f.path)
                    if self.set_dates:
                        dates = re.findall(r"\d{4}-\d{2}-\d{2}", f.path)
                        if len(dates) > 1:
                            date = dates[1]
                        else:
                            date = dates
                        self.date_list.append(date)
                        # self.date_list.append(f.path[-15:-5])
                if self.read_xlsx and f.path.split('.')[-1].lower() == 'xlsx':
                    self.file_list.append(f.path)

    def _read_data(self, filepath, selection, date=None,skiprows=0,delimiter=',',dtype=None):
        """
            Get data from target file
            Args:
                filepath (str): path to file with data.
                selection (str): type of selection.
                date (date): date for data.

            Returns:
                None
        """
        if filepath.split('.')[-1].lower() == 'csv':
            if dtype is not None:
                self.temp_frame = pd.read_csv(filepath, skiprows=skiprows,delimiter=delimiter,dtype=dtype)
            else:
                self.temp_frame = pd.read_csv(filepath, skiprows=skiprows, delimiter=delimiter)
        elif filepath.split('.')[-1].lower() == 'xlsx' and self.read_xlsx:
            dataframes = pd.read_excel(filepath, sheet_name=None)
            for sheet_name, df in dataframes.items():
                if sheet_name != 'list1':
                    pass
                    # df['Sheet_name'] = sheet_name
                self.temp_frame = pd.concat([self.temp_frame, df], sort=False, axis=0)
                # self.temp_frame = pd.read_excel(filepath)
        if self.set_dates:
            self.temp_frame['Date'] = date
        if selection is not None:
            self.temp_frame = self.temp_frame.loc[self.temp_frame[selection[0]] == selection[1]]

    def _concentrate_data(self):
        """Concentrating data into one dataframe"""
        if self.final_frame is None:
            self.final_frame = self.temp_frame
            self.colunm_list = [column for column in self.final_frame]
        else:
            temp_colums = [column for column in self.temp_frame]
            for final_colum, temp_colum in zip(self.colunm_list, temp_colums):
                if final_colum != temp_colum:
                    self.temp_frame.rename(columns={temp_colum: final_colum}, inplace=True)
            self.final_frame = pd.concat([self.temp_frame, self.final_frame], sort=False, axis=0)

    def get_data(self, directory, read_xlsx=False, selection=None, set_dates=True, filepath=None, skip=0,delimiter=',',dtype=None):
        """
            Main function for loading and concentrating from target files in directory.
            Args:
                directory (str): path to directory with target files.
                read_xlsx (bool): if True .xlsx also loaded. Default False.
                selection (): type of selection.
                set_dates (bool): if True get data from file name and add it to file data. Default True.
                filepath (str): path to file with data (to load single file).

            Returns:
                None
        """
        self.set_dates = set_dates
        self.read_xlsx = read_xlsx
        if filepath is None:
            self._create_lists(directory=directory)
        else:
            self.file_list.append(filepath)
            dates = re.findall(r"\d{4}-\d{2}-\d{2}", filepath)
            if len(dates) > 1:
                date = dates[1]
            else:
                date = dates
            self.date_list.append(date)
        if set_dates:
            for filename, date in zip(self.file_list, self.date_list):
                self._read_data(filepath=filename, date=date, selection=selection, skiprows=skip,delimiter=delimiter)
                self._concentrate_data()
        else:
            for filename in self.file_list:
                self._read_data(filepath=filename, selection=selection, skiprows=skip,delimiter=delimiter,dtype=dtype)
                self._concentrate_data()
        return self.final_frame
