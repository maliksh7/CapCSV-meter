import pandas as pd
import numpy as np


class Preprocessing:

    def __init__(self, df):
        self.df = df
        df.replace([np.inf, -np.inf], np.nan, inplace=True)
        df.dropna(inplace=True)
        del df['flow']
        del df['src']
        del df['dst']
        del df['protocol']
        del df['timestamp']
        del df['std_biat']
        del df['furg_cnt']
        del df['burg_cnt']
        del df['total_fhlen']
        del df['total_bhlen']
        del df['flow_urg']
        del df['flow_cwr']
        del df['flow_ece']
        del df['bAvgBytesPerBulk']
        del df['std_active_s']
        del df['min_active_s']
        del df['mean_idle_s']
        del df['std_idle_s']
        del df['max_idle_s']
        del df['min_idle_s']
        del df['label']

    def r_csv(self, filename):
        df = pd.read_csv(filename, encoding='utf-8')
        return df

    def r_hdf(self):
        filename = '/home/bullbat/fyp-2/code/flowmeter/SplitCap/hdf5/data.h5'
        df = pd.read_hdf(filename)
        return df

    def save_to_hdf(self):
        # converting df(csv) to df(HDF5)
        filename = '/home/bullbat/fyp-2/code/flowmeter/SplitCap/hdf5/Normalized-data.h5'
        self.df.to_hdf(filename, 'data', mode='w', format='table')
        print("\nConverted Df to HDF5\n")
        # del df

    def df_info(self):
        # df.head(5)
        print("\nHead of Dataframe: \n")
        print(self.df.head(5))

        # df.shape
        print("\nShape of Dataframe: \n")
        print(self.df.shape)

        # No. of rows and columns in dataframe
        print("\nNumber of Rows in Dataframe: {}\n".format(self.df.shape[0]))
        print("\nNumber of Columns in Dataframe: {}\n".format(
            self.df.shape[1]))

        # # df.info
        # print("\nDataframe Information: \n")
        # self.df.info()

    def columns_in_df(self):
        print("\nColumns in Dataframe: \n")
        col = []
        for i in self.df.columns:
            col.append(i)
        return col

    def dropna(self):
        # df.replace([np.inf, -np.inf], np.nan).dropna(axis=1)
        self.df.replace([np.inf, -np.inf], np.nan, inplace=True)
        # Dropping all the rows with nan valuess
        self.df.dropna(inplace=True)

    # -------------------changing datatypes

    def check_size_dtypes(self, df):

        max = df.max()
        print(max, 'max')

        min = df.min()
        print(min, 'min')
        # print(df.value_counts())
        var1 = df.memory_usage(index=False, deep=True)
        print(var1, 'This is the memory usage')
        print(df.sample(18))

    def convert_datatypes(self, df, a='uint8'):
        print('Trying to convert datatypes for less memory usage')
        max = df.max()
        print(max, 'max')

        min = df.min()
        print(min, 'min')

        # print(df.value_counts())

        var1 = df.memory_usage(index=False, deep=True)
        print(var1, 'memory usage')
        df = df.astype(a)
        var2 = df.memory_usage(index=False, deep=True)
        print(var2, ' new memory usage| the difference -> ', var1 / var2)
        return df

    def normalize(self, df):
        print("[* ] - Normalized data")
        normalized_df = ((df - df.min()) /
                         (df.max() - df.min())) * 225
        return normalized_df

    def apply_fn(self):

        df['src_port'] = d.normalize(df['src_port'])
        df['src_port'] = d.convert_datatypes(df['src_port'])
        d.check_size_dtypes(df['src_port'])

        df['dst_port'] = d.normalize(df['dst_port'])
        df['dst_port'] = d.convert_datatypes(df['dst_port'])
        d.check_size_dtypes(df['dst_port'])

        df['duration'] = d.normalize(df['duration'])
        df['duration'] = d.convert_datatypes(df['duration'])
        d.check_size_dtypes(df['duration'])

        df['total_fpackets'] = d.normalize(df['total_fpackets'])
        df['total_fpackets'] = d.convert_datatypes(df['total_fpackets'])
        d.check_size_dtypes(df['total_fpackets'])

        df['total_bpackets'] = d.normalize(df['total_bpackets'])
        df['total_bpackets'] = d.convert_datatypes(df['total_bpackets'])
        d.check_size_dtypes(df['total_bpackets'])

        df['total_fpktl'] = d.normalize(df['total_fpktl'])
        df['total_fpktl'] = d.convert_datatypes(df['total_fpktl'])
        d.check_size_dtypes(df['total_fpktl'])

        df['total_bpktl'] = d.normalize(df['total_bpktl'])
        df['total_bpktl'] = d.convert_datatypes(df['total_bpktl'])
        d.check_size_dtypes(df['total_bpktl'])

        df['min_fpktl'] = d.normalize(df['min_fpktl'])
        df['min_fpktl'] = d.convert_datatypes(df['min_fpktl'])
        d.check_size_dtypes(df['min_fpktl'])

        df['max_fpktl'] = d.normalize(df['max_fpktl'])
        df['max_fpktl'] = d.convert_datatypes(df['max_fpktl'])
        d.check_size_dtypes(df['max_fpktl'])

        df['mean_fpktl'] = d.normalize(df['mean_fpktl'])
        df['mean_fpktl'] = d.convert_datatypes(df['mean_fpktl'])
        d.check_size_dtypes(df['mean_fpktl'])

        df['std_fpktl'] = d.normalize(df['std_fpktl'])
        df['std_fpktl'] = d.convert_datatypes(df['std_fpktl'])
        d.check_size_dtypes(df['std_fpktl'])

        df['min_bpktl'] = d.normalize(df['min_bpktl'])
        df['min_bpktl'] = d.convert_datatypes(df['min_bpktl'])
        d.check_size_dtypes(df['min_bpktl'])

        df['max_bpktl'] = d.normalize(df['max_bpktl'])
        df['max_bpktl'] = d.convert_datatypes(df['max_bpktl'])
        d.check_size_dtypes(df['max_bpktl'])

        df['mean_bpktl'] = d.normalize(df['mean_bpktl'])
        df['mean_bpktl'] = d.convert_datatypes(df['mean_bpktl'])
        d.check_size_dtypes(df['mean_bpktl'])

        df['std_bpktl'] = d.normalize(df['std_bpktl'])
        df['std_bpktl'] = d.convert_datatypes(df['std_bpktl'])
        d.check_size_dtypes(df['std_bpktl'])

        df['flowBytesPerSecond'] = d.normalize(df['flowBytesPerSecond'])
        df['flowBytesPerSecond'] = d.convert_datatypes(
            df['flowBytesPerSecond'])
        d.check_size_dtypes(df['flowBytesPerSecond'])

        df['flowPktsPerSecond'] = d.normalize(df['flowPktsPerSecond'])
        df['flowPktsPerSecond'] = d.convert_datatypes(
            df['flowPktsPerSecond'])
        d.check_size_dtypes(df['flowPktsPerSecond'])

        df['mean_flowiat'] = d.normalize(df['mean_flowiat'])
        df['mean_flowiat'] = d.convert_datatypes(df['mean_flowiat'])
        d.check_size_dtypes(df['mean_flowiat'])

        df['std_flowiat'] = d.normalize(df['std_flowiat'])
        df['std_flowiat'] = d.convert_datatypes(df['std_flowiat'])
        d.check_size_dtypes(df['std_flowiat'])

        df['max_flowiat'] = d.normalize(df['max_flowiat'])
        df['max_flowiat'] = d.convert_datatypes(df['max_flowiat'])
        d.check_size_dtypes(df['max_flowiat'])

        df['min_flowiat'] = d.normalize(df['min_flowiat'])
        df['min_flowiat'] = d.convert_datatypes(df['min_flowiat'])
        d.check_size_dtypes(df['min_flowiat'])

        df['total_fiat'] = d.normalize(df['total_fiat'])
        df['total_fiat'] = d.convert_datatypes(df['total_fiat'])
        d.check_size_dtypes(df['total_fiat'])

        df['mean_fiat'] = d.normalize(df['mean_fiat'])
        df['mean_fiat'] = d.convert_datatypes(df['mean_fiat'])
        d.check_size_dtypes(df['mean_fiat'])

        df['std_fiat'] = d.normalize(df['std_fiat'])
        df['std_fiat'] = d.convert_datatypes(df['std_fiat'])
        d.check_size_dtypes(df['std_fiat'])

        df['max_fiat'] = d.normalize(df['max_fiat'])
        df['max_fiat'] = d.convert_datatypes(df['max_fiat'])
        d.check_size_dtypes(df['max_fiat'])

        df['min_fiat'] = d.normalize(df['min_fiat'])
        df['min_fiat'] = d.convert_datatypes(df['min_fiat'])
        d.check_size_dtypes(df['min_fiat'])

        df['total_biat'] = d.normalize(df['total_biat'])
        df['total_biat'] = d.convert_datatypes(df['total_biat'])
        d.check_size_dtypes(df['total_biat'])

        df['mean_biat'] = d.normalize(df['mean_biat'])
        df['mean_biat'] = d.convert_datatypes(df['mean_biat'])
        d.check_size_dtypes(df['mean_biat'])

        df['max_biat'] = d.normalize(df['max_biat'])
        df['max_biat'] = d.convert_datatypes(df['max_biat'])
        d.check_size_dtypes(df['max_biat'])

        df['min_biat'] = d.normalize(df['min_biat'])
        df['min_biat'] = d.convert_datatypes(df['min_biat'])
        d.check_size_dtypes(df['min_biat'])

        df['fpsh_cnt'] = d.normalize(df['fpsh_cnt'])
        df['fpsh_cnt'] = d.convert_datatypes(df['fpsh_cnt'])
        d.check_size_dtypes(df['fpsh_cnt'])

        df['bpsh_cnt'] = d.normalize(df['bpsh_cnt'])
        df['bpsh_cnt'] = d.convert_datatypes(df['bpsh_cnt'])
        d.check_size_dtypes(df['bpsh_cnt'])

        df['fPktsPerSecond'] = d.normalize(df['fPktsPerSecond'])
        df['fPktsPerSecond'] = d.convert_datatypes(df['fPktsPerSecond'])
        d.check_size_dtypes(df['fPktsPerSecond'])

        df['bPktsPerSecond'] = d.normalize(df['bPktsPerSecond'])
        df['bPktsPerSecond'] = d.convert_datatypes(df['bPktsPerSecond'])
        d.check_size_dtypes(df['bPktsPerSecond'])

        df['min_flowpktl'] = d.normalize(df['min_flowpktl'])
        df['min_flowpktl'] = d.convert_datatypes(df['min_flowpktl'])
        d.check_size_dtypes(df['min_flowpktl'])

        df['max_flowpktl'] = d.normalize(df['max_flowpktl'])
        df['max_flowpktl'] = d.convert_datatypes(df['max_flowpktl'])
        d.check_size_dtypes(df['max_flowpktl'])

        df['mean_flowpktl'] = d.normalize(df['mean_flowpktl'])
        df['mean_flowpktl'] = d.convert_datatypes(df['mean_flowpktl'])
        d.check_size_dtypes(df['mean_flowpktl'])

        df['std_flowpktl'] = d.normalize(df['std_flowpktl'])
        df['std_flowpktl'] = d.convert_datatypes(df['std_flowpktl'])
        d.check_size_dtypes(df['std_flowpktl'])

        df['var_flowpktl'] = d.normalize(df['var_flowpktl'])
        df['var_flowpktl'] = d.convert_datatypes(df['var_flowpktl'])
        d.check_size_dtypes(df['var_flowpktl'])

        df['flow_fin'] = d.normalize(df['flow_fin'])
        df['flow_fin'] = d.convert_datatypes(df['flow_fin'])
        d.check_size_dtypes(df['flow_fin'])

        df['flow_syn'] = d.normalize(df['flow_syn'])
        df['flow_syn'] = d.convert_datatypes(df['flow_syn'])
        d.check_size_dtypes(df['flow_syn'])

        df['flow_rst'] = d.normalize(df['flow_rst'])
        df['flow_rst'] = d.convert_datatypes(df['flow_rst'])
        d.check_size_dtypes(df['flow_rst'])

        df['flow_psh'] = d.normalize(df['flow_psh'])
        df['flow_psh'] = d.convert_datatypes(df['flow_psh'])
        d.check_size_dtypes(df['flow_psh'])

        df['flow_ack'] = d.normalize(df['flow_ack'])
        df['flow_ack'] = d.convert_datatypes(df['flow_ack'])
        d.check_size_dtypes(df['flow_ack'])

        df['downUpRatio'] = d.normalize(df['downUpRatio'])
        df['downUpRatio'] = d.convert_datatypes(df['downUpRatio'])
        d.check_size_dtypes(df['downUpRatio'])

        df['avgPacketSize'] = d.normalize(df['avgPacketSize'])
        df['avgPacketSize'] = d.convert_datatypes(df['avgPacketSize'])
        d.check_size_dtypes(df['avgPacketSize'])

        df['fAvgSegmentSize'] = d.normalize(df['fAvgSegmentSize'])
        df['fAvgSegmentSize'] = d.convert_datatypes(df['fAvgSegmentSize'])
        d.check_size_dtypes(df['fAvgSegmentSize'])

        df['bAvgSegmentSize'] = d.normalize(df['bAvgSegmentSize'])
        df['bAvgSegmentSize'] = d.convert_datatypes(df['bAvgSegmentSize'])
        d.check_size_dtypes(df['bAvgSegmentSize'])

        df['fAvgBytesPerBulk'] = d.normalize(df['fAvgBytesPerBulk'])
        df['fAvgBytesPerBulk'] = d.convert_datatypes(df['fAvgBytesPerBulk'])
        d.check_size_dtypes(df['fAvgBytesPerBulk'])

        df['fAvgPacketsPerBulk'] = d.normalize(df['fAvgPacketsPerBulk'])
        df['fAvgPacketsPerBulk'] = d.convert_datatypes(
            df['fAvgPacketsPerBulk'])
        d.check_size_dtypes(df['fAvgPacketsPerBulk'])

        df['fAvgBulkRate'] = d.normalize(df['fAvgBulkRate'])
        df['fAvgBulkRate'] = d.convert_datatypes(df['fAvgBulkRate'])
        d.check_size_dtypes(df['fAvgBulkRate'])

        df['bAvgPacketsPerBulk'] = d.normalize(df['bAvgPacketsPerBulk'])
        df['bAvgPacketsPerBulk'] = d.convert_datatypes(
            df['bAvgPacketsPerBulk'])
        d.check_size_dtypes(df['bAvgPacketsPerBulk'])

        df['bAvgBulkRate'] = d.normalize(df['bAvgBulkRate'])
        df['bAvgBulkRate'] = d.convert_datatypes(df['bAvgBulkRate'])
        d.check_size_dtypes(df['bAvgBulkRate'])

        df['fSubFlowAvgPkts'] = d.normalize(df['fSubFlowAvgPkts'])
        df['fSubFlowAvgPkts'] = d.convert_datatypes(df['fSubFlowAvgPkts'])
        d.check_size_dtypes(df['fSubFlowAvgPkts'])

        df['fSubFlowAvgBytes'] = d.normalize(df['fSubFlowAvgBytes'])
        df['fSubFlowAvgBytes'] = d.convert_datatypes(df['fSubFlowAvgBytes'])
        d.check_size_dtypes(df['fSubFlowAvgBytes'])

        df['bSubFlowAvgPkts'] = d.normalize(df['bSubFlowAvgPkts'])
        df['bSubFlowAvgPkts'] = d.convert_datatypes(df['bSubFlowAvgPkts'])
        d.check_size_dtypes(df['bSubFlowAvgPkts'])

        df['bSubFlowAvgBytes'] = d.normalize(df['bSubFlowAvgBytes'])
        df['bSubFlowAvgBytes'] = d.convert_datatypes(df['bSubFlowAvgBytes'])
        d.check_size_dtypes(df['bSubFlowAvgBytes'])

        df['fInitWinSize'] = d.normalize(df['fInitWinSize'])
        df['fInitWinSize'] = d.convert_datatypes(df['fInitWinSize'])
        d.check_size_dtypes(df['fInitWinSize'])

        df['bInitWinSize'] = d.normalize(df['bInitWinSize'])
        df['bInitWinSize'] = d.convert_datatypes(df['bInitWinSize'])
        d.check_size_dtypes(df['bInitWinSize'])

        df['fDataPkts'] = d.normalize(df['fDataPkts'])
        df['fDataPkts'] = d.convert_datatypes(df['fDataPkts'])
        d.check_size_dtypes(df['fDataPkts'])

        df['fHeaderSizeMin'] = d.normalize(df['fHeaderSizeMin'])
        df['fHeaderSizeMin'] = d.convert_datatypes(df['fHeaderSizeMin'])
        d.check_size_dtypes(df['fHeaderSizeMin'])

        df['mean_active_s'] = d.normalize(df['mean_active_s'])
        df['mean_active_s'] = d.convert_datatypes(df['mean_active_s'])
        d.check_size_dtypes(df['mean_active_s'])

        df['max_active_s'] = d.normalize(df['max_active_s'])
        df['max_active_s'] = d.convert_datatypes(df['max_active_s'])
        d.check_size_dtypes(df['max_active_s'])

        df.info()


if __name__ == "__main__":

    filename = '/home/bullbat/fyp-2/code/flowmeter/SplitCap/csvs/merged_data.csv'

    df = pd.read_csv(filename)
    # df.replace([np.inf, -np.inf], np.nan).dropna(axis=1)

    print(df.shape)
    d = Preprocessing(df)
    d.dropna()

    d.apply_fn()

    df.to_csv("preprocessed_csv/preprocessed_data.csv", encoding='utf-8')
    print("\nSaved Preprocessed csv\n")

    d.save_to_hdf()

    d.df_info()
