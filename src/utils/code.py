import numpy as np
import pandas as pd
import math



def temporalKFold(dftmp:pd.DataFrame, date_col:str ="date", num_folds:int=5, shuffle:bool=True, random_state:int=None) -> pd.DataFrame:
    """
    Creates 'num_folds' temporal folds based on the specified 'date_column' in the DataFrame 'data'.
    This function is a valuable tool in spatiotemporal modeling scenarios where multiple measurements
    are recorded on the same date. The purpose is to generate folds that exclude all measurements
    occurring on the same date, aiding in temporal cross-validation.

    Parameters:
    - dftmp (DataFrame): The input DataFrame containing spatiotemporal data.
    - date_column (str): The name of the column representing the temporal information.
    - num_folds (int): The number of temporal folds to create.
    - random_state (int): Set the random seed for reproducibility.

    Returns:
    - blocks_folds_dftmp (DataFrame): A DataFrame include a column representing a temporal fold.
    
    Example:
    >>> data = pd.DataFrame({'Date': pd.date_range('2022-01-01', '2022-01-10'),
                             'Measurement': [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]})
    >>> folds = temporalKFold(data, 'Date', 3)
    """
    if not isinstance(dftmp[date_col].dtype, np.datetime64):
        # Convert the 'Date' column to datetime type
        dftmp[date_col] = pd.to_datetime(dftmp[date_col])

    # Determine the unique dates in the dataframe
    unique_dates = dftmp[date_col].unique()
    split_size = math.ceil(len(unique_dates) / num_folds)

    if shuffle is True:
        if random_state is not None:
            np.random.seed(random_state)
        unique_dates = pd.DatetimeIndex(np.random.permutation(unique_dates))  # Shuffle the dates
    
    blocks_list = [dftmp[dftmp['date'].isin(unique_dates[i * split_size: (i + 1) * split_size])].assign(temp_fold=i+1) for i in range(num_folds)]
    blocks_folds_dftmp = pd.DataFrame(pd.concat(blocks_list))
    return blocks_folds_dftmp

class CustomCrossValidation:
    # References:
    # - Article: Cross-Validation Techniques for Time Series Data
    #   (https://python.plainenglish.io/cross-validation-techniques-for-time-series-data-d1ad7a3a680b)
    # - Blog: Writing Custom Cross-Validation Methods — Grid Search
    #   (https://nander.cc/writing-custom-cross-validation-methods-grid-search)
    
    @classmethod
    def preq_bls(cls, data: pd.DataFrame, date_col: str = "date", n_folds: int = 5):
        """Divide data into blocks. Incrementally slide over blocks. A test block is added to the training block."""
        
        # Ensure the 'date' column is in datetime format
        if not isinstance(data[date_col].dtype, np.datetime64):
            data[date_col] = pd.to_datetime(data[date_col])

        # Get unique dates to use as indices for splitting
        indices = data[date_col].unique()
        
        # Calculate block size based on the number of folds
        block_size = math.ceil(len(indices) / n_folds)
        
        # Generate train/test indices using a sliding window approach
        block_starts = np.arange(0, len(data) + 1, block_size)
        for i in range(n_folds-1):
            train_start = block_starts[0]
            train_end = block_starts[i+1]
            test_start = block_starts[i+1]
            test_end = block_starts[i+2]
            
            # Extract indices for training and testing sets
            train_idx = indices[train_start:train_end]
            test_idx = indices[test_start:test_end]
            
            # Use pandas to filter data based on the selected indices and yield the result
            yield list(data[data[date_col].isin(train_idx)].index), list(data[data[date_col].isin(test_idx)].index)
