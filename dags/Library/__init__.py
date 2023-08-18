
# Build a data frame with the data downloaded
import pandas as pd


def to_dataframe(cursor, columns):
    '''
    This function build a dataframe with the data downloaded by the query and using the columns' name.
    :param cursor: Access to the database
    :param columns: Name of the table's columns
    :return: The DataFrame
    '''

    df = pd.DataFrame(columns=columns)
    for fila in cursor:
        df = pd.concat([df, pd.DataFrame(pd.Series(fila, columns)).transpose()])
    return df.reset_index(drop=True)
