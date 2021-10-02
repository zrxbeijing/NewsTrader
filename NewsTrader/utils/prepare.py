import os
import pandas as pd


def prepare_test(df, data_dir, task, x_column):
    """
    Prepare data for the prediction/test process.
    :param df: a clean and ready for prediction test DataFrame
    :param data_dir: the data directory
    :param task: the task name
    :param x_column: the column that contains X (features).
    :return: None
    """
    if not os.path.exists(data_dir):
        os.mkdir(data_dir)
    cache_dir = os.path.join(data_dir, task)
    if not os.path.exists(cache_dir):
        os.mkdir(cache_dir)
    test_data = df.rename(columns={x_column: "text"})
    # remove empty string ('')
    test_data = test_data[test_data["text"] != ""]
    test_data = test_data.reset_index(drop=True)
    test_data.to_csv(os.path.join(cache_dir, "test.csv"))
    test_data = test_data[["text"]]
    test_data.index.name = "index"
    test_data.to_csv(os.path.join(os.path.join(cache_dir, "test.tsv")), sep="\t")
    return test_data


def prepare_train(df, train_ratio, data_dir, task, x_column, y_column):
    """
    Prepare data for the training/evaluation process
    :param df: a clean and ready for training/evaluation DataFrame
    :param train_ratio: the train and evaluation split ratio
    :param data_dir: the data directory
    :param task: the task name
    :param x_column: the column that contains x (features).
    :param y_column: the column that contains y (labels).
    :return: None
    """
    cache_dir = os.path.join(data_dir, task)
    if not os.path.exists(cache_dir):
        os.mkdir(cache_dir)
    df = df.sample(len(df))
    df = df[df[x_column] != ""]
    train_df = df.iloc[0 : int(len(df) * train_ratio)][[x_column, y_column]]
    train_df.columns = ["text", "labels"]
    dev_df = df.iloc[int(len(df) * train_ratio) :][[x_column, y_column]]
    dev_df.columns = ["text", "labels"]
    train_df.to_csv(os.path.join(cache_dir, "train.tsv"), sep="\t", index=False)
    dev_df.to_csv(os.path.join(cache_dir, "dev.tsv"), sep="\t", index=False)
    return train_df, dev_df
