from glob import glob
import pandas as pd
import requests
import conn_db
import helper
import time

user_agent = helper.user_agent
molit_key = helper.molit_key

def _get_data(url):
    r = requests.get(url, headers={'User-Agent': user_agent})
    try:
        df = pd.DataFrame(r.json()['result_data']['formList'])
        df.rename(columns={'date': '날짜',
                            '호': '값',
                            '사용검사실적': '값',
                            '인허가실적': '값',
                            '착공실적': '값',
                            '미분양현황': '값'}, inplace=True)
        df['날짜'] = df['날짜'].str[:4] + '-' + df['날짜'].str[-2:]
        if '5557' in url:  # 분양승인실적
            df = df.melt(id_vars=['구분1', '구분2', '날짜'],
                            var_name='구분', value_name='값')
        return df
    except:
        print('업데이트 할 내역 없음')

# 새로 받아서 이전 것과 합친 후 저장
def _clean_and_union(url_dict, start_dt, end_dt):
    common_url = f'http://stat.molit.go.kr/portal/openapi/service/rest/getList.do?key={molit_key}&'
    date = f'&start_dt={start_dt}&end_dt={end_dt}'
    try:
        for url in url_dict.keys():
            name = url_dict[url]
            df = _get_data(common_url + url + date)

            # 차원컬럼만 선택해서 컬럼명에 있는 공백제거
            cols = df.select_dtypes('object').columns.tolist()
            for col in df.columns.tolist():
                if col in cols and ' ' in col:
                    names = {col: col.replace(' ', '')}
                    df.rename(columns=names, inplace=True)

            # 이전 data 가져와서 합치기
            old = conn_db.from_('from_국토교통부', name)
            df = pd.concat([df, old], ignore_index=True)
            df.dropna(subset=['값'], inplace=True)

            df['값'] = df['값'].astype('int')

            # 차원컬럼만 선택후 그거 기준으로 중복제거
            cols = df.select_dtypes('object').columns.tolist()
            df = df.drop_duplicates(subset=cols).reset_index(drop=True)

            conn_db.to_(df, 'from_국토교통부', name)
            print(f'{name} 업데이트 완료')
            time.sleep(3)
    except:
        print('Data가 아직 업데이트 되지 않음')
        pass

@helper.timer
def get_data_from_molit_api(start_dt, end_dt):
    '''
    yyyymm 형식으로 시작날짜와 종료 날짜 넣기
    '''
    # 주택건설실적통계_인허가-------------------------------
    url_dict = {'form_id=1948&style_num=1': '주택유형별_인허가실적',
                'form_id=1952&style_num=1': '주택규모별_인허가실적',
                'form_id=1946&style_num=1': '부문별_인허가실적'}
    _clean_and_union(url_dict, start_dt, end_dt)
    print('주택건설 실적통계_인허가 완료')

    # 주택건설실적통계_착공-------------------------------
    url_dict = {'form_id=5387&style_num=1': '주택유형별_착공실적',
                'form_id=5388&style_num=1': '전용면적별_착공실적'}
    _clean_and_union(url_dict, start_dt, end_dt)
    print('주택건설 실적통계_착공 완료')

    # 주택건설실적통계_준공-------------------------------
    url_dict = {'form_id=5373&style_num=1': '주택유형별_사용검사실적',
                'form_id=5374&style_num=1': '전용면적별_사용검사실적'}
    _clean_and_union(url_dict, start_dt, end_dt)
    print('주택건설 실적통계_준공 완료')

    # 주택건설실적통계_분양-------------------------------
    url_dict = {'form_id=5557&style_num=1': '공동주택_분양승인실적'}
    _clean_and_union(url_dict, start_dt, end_dt)
    print('주택건설 실적통계_분양 완료')

    # 미분양현황-------------------------------
    url_dict = {'form_id=2080&style_num=1': '규모별_미분양',
                'form_id=2082&style_num=128': '시군구별_미분양',
                'form_id=5328&style_num=1': '공사완료후_미분양'}
    _clean_and_union(url_dict, start_dt, end_dt)
    print('미분양 현황 완료')
