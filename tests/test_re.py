# test.py
import pandas as pd
from unittest import mock
import os

from spark_air.repartition import re_partition  # Replace 'your_module' with the actual name of your module

# Sample test data
@pytest.fixture
def sample_df():
    return pd.DataFrame({
        'column1': [1, 2],
        'column2': ['A', 'B']
    })

# Mock the file system operations
@mock.patch('pandas.read_parquet')
@mock.patch('pandas.DataFrame.to_parquet')
@mock.patch('os.path.expanduser')
def test_re_partition(mock_expanduser, mock_to_parquet, mock_read_parquet, sample_df):
    # Setup mocks
    mock_expanduser.return_value = '/mock/home'
    mock_read_parquet.return_value = sample_df
    
    load_dt = '20240808'
    from_path = '/data/20240808/movie_data/data/extract'
    
    # Call the function
    size, read_path, write_path = re_partition(load_dt, from_path)
    
    # Check the results
    assert size == sample_df.size
    assert read_path == '/mock/home/data/20240808/movie_data/data/extract/load_dt=20240808'
    assert write_path == '/mock/home/repartition/load_dt=20240808'
    
    # Ensure that the mocked methods were called with expected arguments
    mock_read_parquet.assert_called_once_with('/mock/home/data/20240808/movie_data/data/extract/load_dt=20240808')
    mock_to_parquet.assert_called_once_with(
        '/mock/home/repartition/load_dt=20240808',
        partition_cols=['load_dt', 'multiMovieYn', 'repNationCd']
    )

