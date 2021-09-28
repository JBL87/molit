from glob import glob
import pandas as pd
import requests
import conn_db
import helper
import time

molit_save = conn_db.get_path('molit_savepath')

# 주택 미분양 (시군구별 / 규모별 / 공사완료후)
def clean_unsold():
    # 시군구별 미분양
    df_1 = conn_db.from_('from_국토교통부', '시군구별_미분양')
    df_1['값'] = df_1['값'].astype(int)
    df_1.rename(columns={'구분': '시도',
                        '값': '미분양(호)'}, inplace=True)
    df_1['시군구'] = df_1['시군구'].apply(lambda x: '전체' if x == '계' else x)

    # 규모별 미분양
    df_2 = conn_db.from_('from_국토교통부','규모별_미분양')
    df_2['값'] = df_2['값'].astype(int)
    df_2.rename(columns={'구분': '시도',
                        '값': '미분양(호)'}, inplace=True)
    df_2['시군구'] = '전체'
    df_2['Dataset'] = '규모별 미분양'

    # 공사완료후 미분양
    df_3 = conn_db.from_('from_국토교통부','공사완료후_미분양').dropna()
    df_3['값'] = df_3['값'].astype(int)
    df_3.rename(columns={'구분':'시도',
                        '값':'준공후 미분양(호)'}, inplace=True)
    df_3['시군구'] = df_3['시군구'].str.replace('합계', '전체')
    df_3['시군구'] = df_3['시군구'].apply(lambda x: '전체' if x == '계' else x)

    # 공공부문 소계가 '소계' or '공공부문으로 되어 있는 경우가 있음
    df_3.loc[df_3['부문'] == '공공부문', '규모'] = '소계'
    df_3['규모'] = df_3['규모'].str.replace('∼', '~')
    df_3['Dataset'] = '준공후 미분양'

    # 공사완료후 미분양에서 시도+시군구 전체만 있는거
    df_3_1 = df_3[df_3['부문'] == '계']
    df_3_1 = df_3_1.drop(columns=['부문', '규모']).reset_index(drop=True)

    # 시군구별 미분양 + 시군구별 공사완료후 미분양
    df_1 = pd.concat([df_1, df_3_1])
    df_1['Dataset'] = '시군구별 미분양'
    df_1['부문'] = '전체'
    df_1['규모'] = '전체'
    cols = ['Dataset', '시도', '시군구',
            '날짜', '부문', '규모']
    df_1 = df_1.groupby(by=cols).agg('sum').reset_index()

    # 모두 합치고 지역명 전처리
    df = pd.concat([df_1, df_2, df_3]).reset_index(drop=True)

    # 지역컬럼 수정을 위해 업로드
    cols = ['시도', '시군구']
    temp = df[cols].drop_duplicates()
    temp.sort_values(cols, inplace=True)
    conn_db.to_(temp, 'Master_지역명칭', 'import_국토부미분양')

    # 지역컬럼 수정
    df['key'] = df['시도'] + ' ' + df['시군구']
    df.drop(columns=cols, inplace=True)
    cols = ['key', '시도', '시군구', '시도+시군구']
    region_map = conn_db.from_('Master_지역명칭', '국토부미분양')[cols]
    df = region_map.merge(df, on='key', how='inner').drop(columns='key')

    # 시도에서 전국,수도권 제외
    filt = (df['시도']=='전국') | (df['시도']=='수도권')
    df = df.loc[~filt].copy().reset_index(drop=True)

    # null값 0으로 채우기
    [df[col].fillna(0, inplace=True) for col in ['미분양(호)', '준공후 미분양(호)']]

    cols = ['Dataset', '시도', '시군구', '시도+시군구',
            '날짜', '부문', '규모', ]
    df = df.groupby(by=cols).agg('sum').reset_index()
    df['일반 미분양(호)'] = df['미분양(호)'] - df['준공후 미분양(호)']
    df.loc[df['Dataset']=='준공후 미분양','일반 미분양(호)'] = 0
    df = helper.add_coordinates(df, '시군구')

    for col in ['미분양(호)','준공후 미분양(호)','일반 미분양(호)']:
        df[col] = df[col].astype(int)

    df.to_pickle(molit_save+'국토교통부_미분양.pkl')
    conn_db.export_(df, '국토부_미분양')
    conn_db.to_(df,'국토교통부_미분양현황','data')

# 주택유형별 착공/인허가/준공
def clean_supply_by_type():
    df = pd.DataFrame()
    names = ['착공', '사용검사', '인허가']
    for name in names:
        temp = conn_db.from_('from_국토교통부', f'주택유형별_{name}실적')
        temp['값'] = temp['값'].astype(int)
        temp.rename(columns={'구분': '시도',
                            '시도명': '시도'}, inplace=True)
        temp['구분'] = name
        df = df.append(temp, ignore_index=True)

    for col in ['대분류', '중분류', '소분류']:
        df[col] = df[col].apply(lambda x: x.replace(
            '계(', '합계(') if x[:2] == '계(' in x else x)
        df[col] = df[col].str.replace('합계(동수기준)', '합계(다가구가구수기준)', regex=False)
        df[col] = df[col].str.replace('합계(가구수기준)', '합계(다가구가구수기준)', regex=False)

    df['시도'] = df['시도'].str.replace('총계', '전국')
    df['시도'] = df['시도'].str.replace('수도권소계', '수도권')

    cols = ['시도', '대분류', '중분류', '소분류', '날짜']
    df = df.pivot_table(index=cols, columns='구분', values='값').reset_index()
    df.columns.name = None
    df = df.groupby(by=cols).agg(sum).reset_index()

    for col in ['사용검사','인허가','착공']:
        df[col] = df[col].astype(int)

    df['Dataset'] = '주택유형별'
    df.to_pickle(molit_save+'국토교통부_주택유형별_착공인허가준공.pkl')
    conn_db.to_(df, '국토교통부_공급','주택유형별')

# 전용면적별 착공/인허가/준공
def clean_supply_by_size():
    df = pd.DataFrame()
    names = ['주택규모별_인허가실적', '전용면적별_사용검사실적', '전용면적별_착공실적']
    for name in names:
        temp = conn_db.from_('from_국토교통부', name)
        temp['값'] = temp['값'].astype(int)
        temp.rename(columns={'구분':'시도',
                            '시도별':'시도',
                            '규모별':'규모'}, inplace=True)
        temp['구분'] = name.split('_')[1][:-2]
        df = df.append(temp, ignore_index=True)

    # 시도에서 아래 값들은 제외
    cols = ['전국', '소계', '총계', '수도권소계', '지방소계', '기타광역시', '기타지방']
    for x in cols:
        filt = df['시도'] == x
        df = df.loc[~filt].copy()

    cols = ['권역별', '권역별2']
    [df.drop(columns=col, inplace=True) for col in cols]
    cols = ['시도', '규모', '날짜']
    df = df.pivot_table(index=cols, columns='구분', values='값').reset_index()
    df.columns.name = None
    df = df.groupby(by=cols).agg(sum).reset_index()

    for col in ['사용검사','인허가','착공']:
        df[col] = df[col].astype(int)

    df['Dataset'] = '전용면적별'
    df.to_pickle(molit_save+'국토교통부_전용면적별_착공인허가준공.pkl')

    df['년도']=df['날짜'].str[:4]
    df = df.sort_values(by=['날짜'],ascending=False).reset_index(drop=True)
    df['temp'] = df['시도']+df['규모']+df['년도']
    df_ = pd.DataFrame()
    for x in df['temp']:
        temp = df.loc[df['temp']==x].reset_index(drop=True)
        temp['인허가'] = [(temp['인허가'][i] - temp['인허가'][i+1]) if (i+1)<len(temp) else temp['인허가'][i] for i in range(len(temp))]
        df_ = df_.append(temp, ignore_index=True)
    cols = ['년도','temp']
    [df_.drop(columns=col, inplace=True) for col in cols]
    conn_db.to_(df_, '국토교통부_공급','전용면적별')

# 공공/민간 부문별 인허가
def clean_auth_sell_by_sector():
    df = conn_db.from_('from_국토교통부', '부문별_인허가실적')
    df['값'] = df['값'].astype(int)
    df = df[~(df['부문명'] == '소계')].copy()
    for col in ['부문명', '구분명']:
        df[col] = df[col].str.replace('총  계', '합계')
    df.rename(columns={'시도별': '시도',
                       '값': '인허가',
                       '부문명': '부문',
                       '구분명': '구분'}, inplace=True)
    df.loc[df['구분'] == '합', '구분'] = '소계'
    df['Dataset'] = '부문별_인허가실적'
    df.to_pickle(molit_save+'국토교통부_부문별_인허가.pkl')
    conn_db.to_(df, '국토교통부_공급','인허가')

# 분양승인 실적
def clean_auth_sell():
    df = conn_db.from_('from_국토교통부', '공동주택_분양승인실적')
    df['값'] = df['값'].astype(int)
    df = df[~(df['구분2'] == '소계')].copy()
    df.loc[df['구분2'] == '합계', '구분2'] = '전국'

    df = df.drop(columns='구분1').drop_duplicates().reset_index(drop=True)
    names = {'구분2': '시도',
             '값': '분양승인'}
    df.rename(columns=names, inplace=True)
    df['Dataset'] = '분양승인'
    df.to_pickle(molit_save+'국토교통부_분양승인.pkl')
    conn_db.to_(df, '국토교통부_공급','분양승인')

# for 실거래가 정리용
def _read_trade_file(path):
    df = pd.read_excel(path)
    df = df.dropna().reset_index(drop=True)

    # 첫번째 행을 컬럼명으로 설정하고 첫번째 행 삭제
    df.columns = df.iloc[0]
    df = df.drop(df.index[0]).reset_index(drop=True)
    return df
# for 실거래가 정리용
def _str_to_int(num):
    return int(str(num).replace(',',''))
# 주소 만들기 for 실거래가 정리용
def _make_addr(df):
    df['주소(번지)'] = df['시군구'] + ' ' + df['번지'].astype(str) + ' ' + df['단지명'].astype(str)
    df['단지+층'] = df['단지명'].astype(str) + ' ' + df['층'].astype(str) + '층'

    if '도로명' in df.columns.tolist():
        df['주소(도로명)'] = df['시군구'] + ' ' + df['도로명'].astype(str) + ' ' + df['단지명'].astype(str)

    # 사용하지 않는 컬럼 삭제
    cols = ['번지','본번','부번','도로명']
    for col in cols:
        try:
            df.drop(columns=col, inplace=True)
        except:
            pass
    return df
# for 실거래가 정리용
def _chg_date_col(df):
    df['계약일'] = df['계약일'].astype(int).apply(lambda x: '{:02d}'.format(x))
    df['계약년월'] = df['계약년월'].astype(int).apply(lambda x: '{:04d}'.format(x))

    df['날짜'] = df['계약년월'] + df['계약일']
    df['날짜'] = pd.to_datetime(df['날짜'])
    cols = ['계약년월', '계약일']
    [df.drop(columns=col, inplace=True) for col in cols]
    return df
# for 실거래가 정리용
def _clean_type(trade_type, df):
    df['전용면적(㎡)'] = df['전용면적(㎡)'].astype(float)

    if '매매' in trade_type:
        col_name = '매매가(만원)'
        df.rename(columns={'거래금액(만원)': col_name}, inplace=True)
        df[col_name] = df[col_name].apply(_str_to_int)
        df['구분'] = '매매'

    elif '전월세' in trade_type:
        cols = ['보증금(만원)', '월세(만원)']
        for col in cols:
            df[col] = df[col].apply(_str_to_int)

        df_month = df.loc[df['전월세구분']=='월세'].copy()
        df_month.rename(columns={'보증금(만원)':'월세보증금(만원)'}, inplace=True)

        df_jeonse = df.loc[df['전월세구분']=='전세'].copy()
        df_jeonse.rename(columns={'보증금(만원)':'전세보증금(만원)'}, inplace=True)

        df = pd.concat([df_month, df_jeonse]).reset_index(drop=True)
        df.rename(columns={'전월세구분':'구분'}, inplace=True)
        del df_month, df_jeonse

    elif '분양권' in trade_type:
        col_name = '분양권(만원)'
        df.rename(columns={'거래금액(만원)': col_name}, inplace=True)
        df[col_name] = df[col_name].apply(_str_to_int)

        df['분/입구분'] = df['분/입구분'].apply(lambda x : '분양권' if x=='분' else'입주권')
        df.rename(columns={'분/입구분':'구분'}, inplace=True)

    cols = ['건축년도','층','시군구']
    for col in cols:
        try:
            df[col]= df[col].astype('category')
        except:
            pass
    return df

# 국토부 실거래가 전처리
def clean_molit_real_trade_price():
    '''
    국토부 실거래가 전처리
    '''
    trade_types = ['실거래가_아파트_매매', '실거래가_아파트_전월세', '실거래가_아파트_분양권']
    for trade_type in trade_types:
        paths = glob(conn_db.get_path(trade_type)+ "*.xlsx")
        df = pd.concat([_read_trade_file(path) for path in paths])
        df = _make_addr(df.reset_index(drop=True))

        df = _chg_date_col(df) # 날짜컬럼 생성
        df = _clean_type(trade_type, df) # 구조통일, 컬럼명 수정

        # 저장
        df.to_pickle(molit_save + trade_type + '.pkl')
        print(f'{trade_type} 취합 완료')

    # # 지역명칭 df
    # df_addr = df['시군구'].str.split(' ', expand=True)
    # df_addr = pd.concat([df_addr, df['시군구']], axis=1)
    # df_addr = df_addr.drop_duplicates().reset_index(drop=True)
    # names = {'시군구':'key',
    #         0:'시도',
    #         1:'시군구',
    #         2:'읍면동'}
    # df_addr.rename(columns=names, inplace=True)

    # # 읍면동 컬럼 정리
    # filt = df_addr[3].notna()
    # temp = df_addr.loc[filt].copy()
    # temp.loc[ : ,'읍면동'] = temp['읍면동'] + ' ' + temp[3]

    # df_addr = df_addr.loc[~filt].copy()
    # df_addr = pd.concat([temp, df_addr]).reset_index(drop=True)

    # # 필요없는 컬럼 삭제 (읍면동 2번째 컬럼)
    # df_addr.drop(columns=3, inplace=True)

    # # 시도+시군구만
    # temp = df_addr[['시도','시군구']]
    # temp = temp.drop_duplicates().reset_index(drop=True)
    # temp['key'] = temp['시도'] + temp['시군구']

