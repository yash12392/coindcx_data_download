"""

Author: Trade With Python (Yash Roongta)
The objective of this app is to help users download crypto ticker data using the CoinDCX APIs

"""

from datetime import datetime, timedelta
import time, math, concurrent.futures, requests
import pandas as pd
import streamlit as st
import base64, os, json, pickle, uuid, re, io
import streamlit.components.v1 as components


def local_time(epoch_time):
    return time.strftime('%d-%m-%Y %H:%M:%S', time.localtime(int(str(epoch_time)[0:10])))

def coindcx_urls(startDate, endDate, token, interval):

    urls_to_scrape = list()

    epoch_startDate = startDate.timestamp()
    epoch_endDate = endDate.timestamp()

    if interval == '1m':
        range_end = math.ceil((epoch_endDate - epoch_startDate)/60000)
    elif interval == '5m':
        range_end = math.ceil((epoch_endDate - epoch_startDate)/(60000 * 5))
    elif interval == '15m':
        range_end = math.ceil((epoch_endDate - epoch_startDate)/(60000 * 15))
    elif interval == '30m':
        range_end = math.ceil((epoch_endDate - epoch_startDate)/(60000 * 30))
    elif interval == '1h':
        range_end = math.ceil((epoch_endDate - epoch_startDate)/(60000 * 60))
    elif interval == '2h':
        range_end = math.ceil((epoch_endDate - epoch_startDate)/(60000 * 120))
    elif interval == '4h':
        range_end = math.ceil((epoch_endDate - epoch_startDate)/(60000 * 240))
    elif interval == '6h':
        range_end = math.ceil((epoch_endDate - epoch_startDate)/(60000 * 360))
    elif interval == '8h':
        range_end = math.ceil((epoch_endDate - epoch_startDate)/(60000 * 480))
    elif interval == '1d':
        range_end = math.ceil((epoch_endDate - epoch_startDate)/(60000 * 1440))
    elif interval == '3d':
        range_end = math.ceil((epoch_endDate - epoch_startDate)/(60000 * 1440 * 3))
    elif interval == '1w':
        range_end = math.ceil((epoch_endDate - epoch_startDate)/(60000 * 1440 * 7))
    elif interval == '1M':
        range_end = math.ceil((epoch_endDate - epoch_startDate)/(60000 * 1440 * 30))

    for i in range(0,range_end):

        if interval == '1m':
            url_endDate = (datetime.fromtimestamp(epoch_startDate) + timedelta(minutes = 1000)).timestamp()
        elif interval == '5m':
            url_endDate = (datetime.fromtimestamp(epoch_startDate) + timedelta(minutes = 5000)).timestamp()
        elif interval == '15m':
            url_endDate = (datetime.fromtimestamp(epoch_startDate) + timedelta(minutes = 15000)).timestamp()
        elif interval == '30m':
            url_endDate = (datetime.fromtimestamp(epoch_startDate) + timedelta(minutes = 30000)).timestamp()
        elif interval == '1h':
            url_endDate = (datetime.fromtimestamp(epoch_startDate) + timedelta(hours = 1000)).timestamp()
        elif interval == '2h':
            url_endDate = (datetime.fromtimestamp(epoch_startDate) + timedelta(hours = 2000)).timestamp()
        elif interval == '4h':
            url_endDate = (datetime.fromtimestamp(epoch_startDate) + timedelta(hours = 4000)).timestamp()
        elif interval == '8h':
            url_endDate = (datetime.fromtimestamp(epoch_startDate) + timedelta(hours = 8000)).timestamp()
        elif interval == '1d':
            url_endDate = (datetime.fromtimestamp(epoch_startDate) + timedelta(days = 1000)).timestamp()
        elif interval == '3d':
            url_endDate = (datetime.fromtimestamp(epoch_startDate) + timedelta(days = 3000)).timestamp()
        elif interval == '1w':
            url_endDate = (datetime.fromtimestamp(epoch_startDate) + timedelta(weeks = 1000)).timestamp()
        elif interval == '1M':
            url_endDate = (datetime.fromtimestamp(epoch_startDate) + timedelta(days = 30000)).timestamp()

        if epoch_endDate > url_endDate:
            urls_to_scrape.append(f"https://public.coindcx.com/market_data/candles?pair={token}&interval={interval}&startTime={int(epoch_startDate)}000&endTime={int(url_endDate)}000&limit=1000")

            epoch_startDate = url_endDate

        if epoch_endDate < url_endDate:
            urls_to_scrape.append(f"https://public.coindcx.com/market_data/candles?pair={token}&interval={interval}&startTime={int(epoch_startDate)}000&endTime={int(epoch_endDate)}000&limit=1000")

    return urls_to_scrape

def get_single_data(url):

    resp = requests.get(url)
    data = resp.json()

    return pd.DataFrame(data)

def get_multiple_data(urls):

    results = list()

    with concurrent.futures.ThreadPoolExecutor() as executor:
        for result in executor.map(get_single_data, urls):
            results.append(result)

    final_df = pd.concat(results)

    if not final_df.empty:
        final_df['date_time'] = final_df.apply(lambda x: local_time(x['time']), axis=1)
        final_df.sort_values('time', inplace = True)
        final_df.reset_index(drop = True, inplace = True)
        final_df = final_df[['date_time', 'open', 'high', 'low', 'close', 'volume']]
        final_df.drop_duplicates(subset = ['date_time'], inplace = True)

    return final_df

def exchange_code(ecode):
    if ecode == 'B':
        return 'Binance'
    elif ecode == 'I':
        return 'CoinDCX'
    elif ecode == 'HB':
        return 'HitBTC'
    elif ecode == 'H':
        return 'Huobi'
    elif ecode == 'BM':
        return 'BitMEX'

@st.cache(allow_output_mutation=True)
def get_all_coindcx_data():
    url = "https://api.coindcx.com/exchange/v1/markets_details"

    response = requests.get(url)
    data = response.json()

    df = pd.DataFrame(data)
    df['target_currency_st'] = df.apply(lambda x: x['target_currency_short_name'] + ' - ' + x['target_currency_name'], axis=1)
    df['base_currency_st'] = df.apply(lambda x: x['base_currency_short_name'] + ' - ' + x['base_currency_name'], axis=1)
    df['exchange_code'] = df.apply(lambda x: exchange_code(x['ecode']), axis=1)

    return df

def download_button(object_to_download, download_filename, button_text, pickle_it=False):

    if pickle_it:
        try:
            object_to_download = pickle.dumps(object_to_download)
        except pickle.PicklingError as e:
            st.write(e)
            return None

    else:
        if isinstance(object_to_download, bytes):
            pass

        elif isinstance(object_to_download, pd.DataFrame):
            #object_to_download = object_to_download.to_csv(index=False)
            towrite = io.BytesIO()
            object_to_download = object_to_download.to_csv(towrite, encoding='utf-8', index=False, header=True)
            towrite.seek(0)

        # Try JSON encode for everything else
        else:
            object_to_download = json.dumps(object_to_download)

    try:
        # some strings <-> bytes conversions necessary here
        b64 = base64.b64encode(object_to_download.encode()).decode()

    except AttributeError as e:
        b64 = base64.b64encode(towrite.read()).decode()

    button_uuid = str(uuid.uuid4()).replace('-', '')
    button_id = re.sub('\d+', '', button_uuid)

    custom_css = f""" 
        <style>
            #{button_id} {{
                display: inline-flex;
                align-items: center;
                justify-content: center;
                background-color: rgb(255, 255, 255);
                color: rgb(38, 39, 48);
                padding: .25rem .75rem;
                position: relative;
                text-decoration: none;
                border-radius: 4px;
                border-width: 1px;
                border-style: solid;
                border-color: rgb(230, 234, 241);
                border-image: initial;
            }} 
            #{button_id}:hover {{
                border-color: rgb(246, 51, 102);
                color: rgb(246, 51, 102);
            }}
            #{button_id}:active {{
                box-shadow: none;
                background-color: rgb(246, 51, 102);
                color: white;
                }}
        </style> """

    dl_link = custom_css + f'<a download="{download_filename}" id="{button_id}" href="data:application/vnd.openxmlformats-officedocument.spreadsheetml.sheet;base64,{b64}">{button_text}</a><br></br>'

    return dl_link


if __name__ == '__main__':

    st.set_page_config(page_title='TradeWithPython - CoinDCX Downloader', page_icon=None, layout='centered', initial_sidebar_state='auto')

    st.image('./twp_logo.png')

    st.title('CoinDCX APIs Data Download :chart_with_upwards_trend:')

    market_df = get_all_coindcx_data()

    exchanges = market_df['exchange_code'].unique()
    selected_exchange = st.selectbox(label = 'Which Exchange Would you like to download data for?', options = exchanges, index = 2)
    exchange_df = market_df[market_df['exchange_code'] == selected_exchange]

    col1, col2 = st.columns(2)

    with col1:
        available_target_ccy = exchange_df['target_currency_st'].unique()
        selected_target_ccy = st.selectbox("Which Coin Would you like to select?", options = available_target_ccy)
        target_currency_df = exchange_df[exchange_df['target_currency_st'] == selected_target_ccy]

    with col2:
        available_base_ccy = target_currency_df['base_currency_st'].unique()
        selected_base_ccy = st.selectbox("Which Base Currency Would you like to select?", options = available_base_ccy)
        available_base_df = target_currency_df[target_currency_df['base_currency_st'] == selected_base_ccy]

    with st.form("coindcx_form"):
            col3, col4 = st.columns(2)

            with col3:
                from_date = st.date_input(label = 'Select From Date', value = datetime(2021,1,1), min_value = datetime(2018,1,1), max_value = (datetime.now() - timedelta(days=1)))
                to_date = st.date_input(label = 'Select To Date', value = datetime.now(), min_value = from_date, max_value = datetime.now())
                
            with col4:
                from_time = st.time_input(label = "Select From Time", value = datetime(2021,1,1,0,0))
                to_time = st.time_input(label = "Select To Time", value = datetime(2021,1,1,23,59))

            from_date_time = datetime.combine(from_date, from_time)
            to_date_time = datetime.combine(to_date, to_time)
        
            frequency = st.selectbox(label = "Which Frequency Would you like to Download Data For?", options = ['1m', '5m', '15m', '30m', '1h', '2h', '4h', '6h', '8h', '1d', '3d', '1w', '1M'], index = 1,
                                        help = "If you select a large timeframe with 1m timeframe, it might take some while to download all the data.")

            submit_btn = st.form_submit_button("Get Data")

    placeholder1 = st.empty()
    placeholder1.warning("Please note due to technology restrictions, we cannot allow downloading files greater than **50MB**, so please select smaller time frames to generate data and then download it. **You can easily do 1 Year worth data within 50MB limit.**")
    
    if submit_btn:

        placeholder1.info("Thank You For Filling In Form, We will start getting your data now using the CoinDCX Public APIs.")
        time.sleep(1)
        placeholder1.info(f"We will be fetching {frequency} frequency data for pair {selected_target_ccy}/{selected_base_ccy} for {selected_exchange}")
        time.sleep(1)

        token = available_base_df['pair'].iloc[0]

        urls = coindcx_urls(from_date_time, to_date_time, token, frequency)
        placeholder1.info(f"We need to fetch your requested data from {len(urls)} URLs, please be patient while we do this.")
        time.sleep(1)
        
        placeholder1.info(f"Fetching Data Now... We will have to make {len(urls)} API calls to get this for you...")
        coindcx_df = get_multiple_data(urls)
        placeholder1.success(f"Data Fetching Complete, we downloaded {len(coindcx_df)} lines of data for you. You can download the file using the button below. Depending on the file size, it might take a few seconds for the button to appear.")

        st.warning("Please note, we have only downloaded the data that was provided by CoinDCX, if you have missing ticks, that's because there was no data for the pair you requested.")

        st.warning("Please note, if you are downloading the file on a **mobile phone**, it automatically converts the **.csv** file to **.xlsx** file which corrupts it, you can remove **.xlsx** by renaming the file and open it. That should work. If you are downloading the data on desktop, you should face no problems!")

        placeholder2 = st.empty()

        placeholder2.info("Generating Button...")
        
        download_button_str = download_button(coindcx_df, f'{token}_{int(from_date_time.strftime("%d%m%Y%H%M%S"))}_{int(to_date_time.strftime("%d%m%Y%H%M%S"))}.csv', 'Download Your Data')

        placeholder2.markdown(download_button_str, unsafe_allow_html = True)

    st.header("Know More About Trade With Python")

    with st.expander(label = "Click Here to Expand", expanded = True):
        st.write("""**Trade With Python** started in February 2021 with the mission to educate **Indians** about Algo - Trading by specifically using tools and strategies that work 
                    in the Indian market. Our blogs are very simple to understand and are written by contributors who know Finance and Technology.""")

        st.markdown("""**Website:** https://tradewithpython.com""", unsafe_allow_html= True)

        st. markdown("""**This project is open-sourced on Github:** https://github.com/yash12392/coindcx_data_download""", unsafe_allow_html= True)
        st.markdown("""**Do give it a :star: and feel free to fork and suggest any additional improvements.**""", unsafe_allow_html= True)

        st.write("""We request you to fill in the Contact Us Form on the below page if you have any feedback on this app or on our blogs :smiley:""")

        st.write("""**If you would like Trade With Python to write blogs and create Youtube Videos on how we made this app, please fill the Contact Us form to show your interest.**""")

        st.markdown("""Contact Us Page: https://tradewithpython.com/contact-us""", unsafe_allow_html= True)

        st.write("""**Founder: Yash Roongta**""")
        st.markdown("""Reach out to Yash on Linkedin: https://www.linkedin.com/in/yashroongta/""")
        st.markdown("""Reach out to Yash on Email :email:: mailto:yash@tradewithpython.com """)

