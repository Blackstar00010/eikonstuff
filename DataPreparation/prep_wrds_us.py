import pandas as pd
import Misc.useful_stuff as us
from pathlib import Path
import platform


def collapse_dup(some_df: pd.DataFrame, subset: list = None, df_name: str = None) -> pd.DataFrame:
    """
    Use parallel computing to groupby based on subset and collapse duplicates. Only for us data.
    :param some_df: dataframe with duplicated rows
    :param subset: subset of columns to consider duplicates
    :param df_name: name of dataframe. If not None, prints progress
    :return: dataframe with duplicates collapsed
    """
    if subset is None:
        subset = some_df.columns.tolist()
    some_df = some_df.sort_values(by=subset).reset_index(drop=True)

    # separating dirty and clean parts so that we only have to compute the dirty part
    dirty_index = some_df.index[some_df.duplicated(subset, keep=False)]
    dirty_part = some_df.loc[dirty_index, :]
    ret = some_df.drop(dirty_index, axis=0)

    # dropping all-NA rows
    dirty_part = dirty_part.dropna(subset=[col for col in dirty_part.columns if col not in subset], how='all')
    dirty_part = dirty_part.sort_values(by=subset).reset_index(drop=True)

    # ffill-ing each groups of duplicates
    dirty_part['group_no'] = dirty_part.groupby(subset).ngroup()
    dirty_part = dirty_part.groupby('group_no').apply(lambda group: group.fillna(method='ffill')).reset_index(drop=True)
    dirty_part = dirty_part.drop_duplicates(subset=subset, keep='last').drop('group_no', axis=1)

    ret = pd.concat([ret, dirty_part], axis=0).sort_values(by=subset).reset_index(drop=True)
    if df_name is not None:
        print(f'Finished managing duplicates of {df_name} data!')
    return ret


if __name__ == '__main__':
    convert_to_csv = True
    if convert_to_csv:
        files_dir = '/Users/jamesd/Desktop/wrds_data/us/'
        characteristics_dir = '../data/processed_wrds_us/'

        all_df = pd.read_csv(files_dir + 'temp2.csv', low_memory=False)

        # join all data
        target_cols = ['permno', 'gvkey', 'datadate', 'absacc', 'acc', 'aeavol', 'age', 'agr', 'baspread', 'beta',
                       'betasq', 'bm', 'bm_ia', 'cash', 'cashdebt', 'cashpr', 'cfp', 'cfp_ia', 'chatoia', 'chcsho',
                       'chempia', 'chinv', 'chmom', 'chpmia', 'chtx', 'cinvest', 'convind', 'currat', 'depr', 'divi',
                       'divo', 'dolvol', 'dy', 'ear', 'egr', 'ep', 'gma', 'herf', 'hire', 'idiovol', 'ill', 'indmom',
                       'invest', 'IPO', 'lev', 'lgr', 'maxret', 'ms', 'mve', 'mve_ia', 'nincr', 'operprof',
                       'pchcapx_ia', 'pchcurrat', 'pchdepr', 'pchgm_pchsale', 'pchquick', 'pchsale_pchrect', 'pctacc',
                       'pricedelay', 'ps', 'quick', 'rd', 'retvol', 'roaq', 'roeq', 'roic', 'rsup', 'salecash', 'salerec', 'secured', 'securedind', 'sgr',
                       'sin', 'sp', 'std_dolvol', 'std_turn', 'sue', 'tang', 'tb', 'turn', 'zerotrade']
        all_df = all_df.drop(columns=[item for item in all_df.columns if item not in target_cols])

        # permno to join with close
        permno_df = all_df[['permno', 'gvkey']].drop_duplicates(subset=['permno', 'gvkey'], keep='first')
        permno_df = permno_df[permno_df['permno'].notna()]
        permno_df = permno_df.drop_duplicates(subset=['permno'], keep='last')
        all_df = all_df[all_df['permno'].isin(permno_df['permno'])]
        all_df = all_df.merge(permno_df, on='gvkey')
        all_df['permno_x'] = all_df['permno_x'].fillna(all_df['permno_y'])
        all_df = all_df.drop('permno_y', axis=1).rename(columns={'permno_x': 'permno'})

        # match format & collapse duplicates
        all_df = all_df.dropna(subset=['datadate'])
        all_df = us.change_date_format(all_df, df_name='all_df', date_col_name='datadate')
        all_df['datadate'] = all_df['datadate'].str[:7]
        all_df = collapse_dup(all_df, subset=['gvkey', 'datadate'], df_name='all_df')  # todo: gvkey -> permno?

        # close price -> momentum
        close_df = pd.read_csv(files_dir + 'close.csv', low_memory=False)
        close_df = close_df.rename(columns={'PERMNO': 'permno', 'PRC': 'prc', 'DATE': 'datadate'})
        close_df = us.change_date_format(close_df, df_name='close_df', date_col_name='datadate')
        close_df = close_df[close_df['permno'].isin(permno_df['permno'])]
        close_df = close_df[close_df['prc'] > 0]
        close_df = close_df[close_df['datadate'].str[5:7] != close_df['datadate'].str[5:7].shift(-1)]
        close_df['datadate'] = close_df['datadate'].str[:7]  # YYYY-MM-DD -> YYYY-MM
        close_df['prc1'] = close_df.groupby('permno')['prc'].shift(1)
        close_df['mom1'] = close_df['prc'] / close_df['prc1'] - 1
        for i in range(2, 50):
            close_df[f'mom{i}'] = close_df.groupby('permno')['prc'].shift(i)
            close_df[f'mom{i}'] = close_df['prc1'] / close_df[f'mom{i}'] - 1
        us.pivot(close_df[['datadate', 'permno', 'prc']], df_name='prc_df',
                 col_col='permno', row_col='datadate').to_csv(characteristics_dir + '_close.csv', index=True)
        close_df = close_df.drop(columns=['prc', 'prc1'])

        # exporting mom1 data
        mom1_df = us.pivot(close_df[['datadate', 'permno', 'mom1']], df_name='mom_df',
                           col_col='permno', row_col='datadate')
        # mom1_df = mom1_df[mom1_df.index.str[:4].astype(int) > 1970]
        mom1_df.to_csv(characteristics_dir+'_momentum.csv', index=True)

        # because the firms have data quarterly, not monthly,
        # we need to ffill at most 3 months
        # todo: all_df = all_df.fillna(method='ffill', limit=3)? would it work?
        one_month_ago = pd.DataFrame()
        two_months_ago = pd.DataFrame()
        three_months_ago = pd.DataFrame()
        dates = pd.Series(close_df['datadate'].unique()).sort_values()
        dates = dates[dates >= all_df['datadate'].min()]
        for adate in dates:
            zero_month_ago = all_df[all_df['datadate'] == adate].drop('datadate', axis=1)
            zero_month_ago['firms'] = zero_month_ago['permno'].astype(int)
            zero_month_ago = zero_month_ago.drop_duplicates(subset=['firms'])
            zero_month_ago = zero_month_ago.set_index('firms').drop(columns=['permno', 'gvkey'])
            this_month_price_df = close_df[close_df['datadate'] == adate].drop('datadate', axis=1)
            this_month_price_df['firms'] = this_month_price_df['permno'].astype(int)
            this_month_price_df = this_month_price_df.drop_duplicates(subset=['firms'])
            this_month_price_df = this_month_price_df.set_index('firms').drop(columns='permno')

            this_month_df = pd.concat([zero_month_ago,
                                       one_month_ago.drop(zero_month_ago.index, errors='ignore')], axis=0)
            this_month_df = pd.concat([this_month_df,
                                       two_months_ago.drop(this_month_df.index, errors='ignore')], axis=0)
            for afirm in this_month_df.index:
                if (afirm in three_months_ago.index) and (afirm not in this_month_df.index):
                    this_month_df = this_month_df.drop(index=afirm)
            three_months_ago, two_months_ago, one_month_ago = two_months_ago, one_month_ago, zero_month_ago

            to_export = pd.concat([this_month_df,
                                   this_month_price_df.loc[this_month_price_df.index.isin(this_month_df.index), :]],
                                  axis=1)
            to_export.to_csv(characteristics_dir + f'/_{adate}.csv', index=True, index_label='firms')

    clean_data = True
    if clean_data:
        characteristics_dir = '../data/processed_wrds_us/'
        first_days = [item for item in us.listdir(characteristics_dir) if item[0] == '_']
        for afirst_day in first_days:
            if 'mom' in afirst_day:
                continue
            chars_df = pd.read_csv(characteristics_dir + afirst_day, low_memory=False)
            if len(chars_df) < 1:
                continue
            chars_df = chars_df.set_index('firms')
            moms_df = chars_df
            for acol in moms_df.columns:
                if 'mom' in acol:
                    chars_df = chars_df.drop(acol, axis=1)
                else:
                    moms_df = moms_df.drop(acol, axis=1)

            best_case_finder = []
            chars_df_og = chars_df
            for thresh_col in range(10, 1, -1):
                chars_df = chars_df_og
                # drop columns(characteristics) with too many NaNs
                for acol in chars_df.columns:
                    # shitty column based on thresh_col
                    if chars_df[acol].notna().sum() < (len(chars_df) * thresh_col / 10):
                        chars_df = chars_df.drop(acol, axis=1)

                # drop rows(firms) with too many NaNs
                thresh_row = 1.0
                temp_df = chars_df.dropna(thresh=int(chars_df.shape[1] * thresh_row), axis=0)
                while len(temp_df) < (len(chars_df) / 2):
                    thresh_row -= 0.1
                    temp_df = chars_df.dropna(thresh=int(chars_df.shape[1] * thresh_row), axis=0)
                temp_df = temp_df.dropna(how='any', axis=1)

                best_case_finder.append([thresh_col, thresh_row, temp_df.shape[0] * temp_df.shape[1]])
            best_case_finder = pd.DataFrame(best_case_finder, columns=['thresh_col', 'thresh_row', 'valid_count'])
            best_case_finder = best_case_finder.sort_values(by=['valid_count', 'thresh_col'], ascending=False)

            chars_df = chars_df_og
            for acol in chars_df.columns:
                if chars_df[acol].notna().sum() < (len(chars_df) * best_case_finder.iloc[0, 0] / 10):
                    chars_df = chars_df.drop(acol, axis=1)
            chars_df = chars_df.dropna(thresh=int(chars_df.shape[1] * best_case_finder.iloc[0, 1]), axis=0)

            chars_df = pd.concat([chars_df, moms_df], axis=1)
            chars_df = chars_df.dropna(how='any', axis=0)
            chars_df.to_csv(characteristics_dir + afirst_day[1:], index=True, index_label='firms')
            # print(f'{afirst_day} exported!')