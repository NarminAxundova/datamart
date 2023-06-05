import pandas as pd
from io import BytesIO
import streamlit as st
import psycopg2
from st_aggrid import AgGrid, GridOptionsBuilder, DataReturnMode,ColumnsAutoSizeMode,GridUpdateMode
from datetime import datetime,timedelta
from config import config
import math

converted = str()
dictionary=config()
# iterating over dictionary using a for loop
for key in dictionary:
    converted += key + "= '" + dictionary[key] + "' "
converted=converted[:len(converted)-1]
#@st.cache(allow_output_mutation=True)
def connect(func):
    def connection(*args, **kwargs):
        # Here, you may even use a connection pool
        conn = psycopg2.connect(converted)
        try:
            rv = func(conn, *args, **kwargs)
        except Exception as e:
            conn.rollback()
            raise e
        else:
                # Can decide to see if you need to commit the transaction or not
            conn.commit()
        finally:
            conn.close()
        return rv
    return connection
st.cache(ttl=60)
def dataframee(df):
    gb = GridOptionsBuilder.from_dataframe(df)
    gb.configure_grid_options(groupRowAggFunction='sum')
    gb.configure_default_column(enablePivot=True, enableValue=True, enableRowGroup=True)
    gb.configure_side_bar()
    custom_css = {
    ".ag-row-hover": {"background-color": "orange !important","color":"orange !important"},
    ".ag-header-cell-label": {"color": "orange !important"},
    ".ag-header": {"background-color": "blue !important;"},
    ".ag-cell-value": {"color": "blue !important;"}
    }
    gb.configure_pagination(paginationAutoPageSize=100)
    gb.configure_grid_options(enableServerSideSorting=True, enableServerSideFilter=True)
    #gb.configure_selection(selection_mode="multiple", use_checkbox=True)
    

    gridoptions = gb.build()

    response = AgGrid(
                    df,
                    height=600,
                    gridOptions=gridoptions,
                    enable_enterprise_modules=True,
                    update_mode=GridUpdateMode.SELECTION_CHANGED,
                    data_return_mode=DataReturnMode.FILTERED_AND_SORTED,
                    fit_columns_on_grid_load=False,
                    header_checkbox_selection_filtered_only=True,
                    custom_css=custom_css,
                    use_checkbox=True)
st.cache(ttl=60)
def dataframee1(df):
    gb = GridOptionsBuilder.from_dataframe(df)
    gb.configure_grid_options(enableServerSideSorting=True, enableServerSideFilter=True)
    gb.configure_grid_options(groupRowAggFunction='sum')
    gb.configure_default_column(enablePivot=True, enableValue=True, enableRowGroup=True)
   
    custom_css = {
    ".ag-row-hover": {"background-color": "orange !important","color":"orange !important"},
    ".ag-header-cell-label": {"color": "orange !important"},
    ".ag-header": {"background-color": "blue !important;"},
    ".ag-cell-value": {"color": "blue !important;"}
    }
    
    #gb.configure_selection(selection_mode="multiple", use_checkbox=True)
    gridoptions = gb.build()

    response = AgGrid(
                    df,
                    height=100,
                    gridOptions=gridoptions,
                    enable_enterprise_modules=True,
                    update_mode=GridUpdateMode.SELECTION_CHANGED,
                    data_return_mode=DataReturnMode.FILTERED_AND_SORTED,
                    fit_columns_on_grid_load=True,
                    header_checkbox_selection_filtered_only=True,
                    custom_css=custom_css,
                    use_checkbox=True)
                    #columns_auto_size_mode=ColumnsAutoSizeMode.FIT_ALL_COLUMNS_TO_VIEW)
st.cache(ttl=60)
def dataframmee(df):
    gb = GridOptionsBuilder.from_dataframe(df)
    gb.configure_grid_options(groupRowAggFunction='sum',pivotMode= True)
    gb.configure_default_column(enablePivot=True, enableValue=True, enableRowGroup=True)
    gb.configure_column('id_service',enableRowGroup= True, enablePivot= True, Filter=False  ) 
    gb.configure_side_bar()
    gb.configure_grid_options(enableServerSideSorting=True, enableServerSideFilter=True)
    custom_css = {
    ".ag-row-hover": {"background-color": "orange !important","color":"orange !important"},
    ".ag-header-cell-label": {"color": "orange !important"},
    ".ag-header": {"background-color": "blue !important;"},
    ".ag-cell-value": {"color": "blue !important;"}
    }
    gb.configure_pagination(paginationAutoPageSize=100)
    #gb.configure_selection(selection_mode="multiple", use_checkbox=True)
    grid_options = {
    'rowHeight': 50,
    'enableSorting': True,
    'enableFilter': True,
    'enableColResize': True,
    'enableServerSideSorting': True,
    'enableServerSideFilter': True,
    'pagination': True,
     'sidebar': True,
    'paginationAutoPageSize': True,
    'pivotMode': True,
    'enablePivot': True,
    'enableValue': True,
    'enableRowGroup': True,
    'rowGroupPanelShow': 'always',
    'suppressAggFuncInHeader': True,
    'suppressMakeColumnVisibleAfterUnGroup': True,
    'rowSelection': 'multiple'
    }
    gridoptions = gb.build()

    response = AgGrid(
                    df,
                    height=600,
                    gridOptions=gridoptions,
                    enable_enterprise_modules=True,
                    update_mode=GridUpdateMode.SELECTION_CHANGED,
                    data_return_mode=DataReturnMode.FILTERED_AND_SORTED,
                    fit_columns_on_grid_load=False,
                    header_checkbox_selection_filtered_only=True,
                    custom_css=custom_css,
                    use_checkbox=True)
st.cache(ttl=60)
def dataframmee111(df):
    gb = GridOptionsBuilder.from_dataframe(df)
    gb.configure_grid_options(groupRowAggFunction='sum')
    gb.configure_default_column(enablePivot=True, enableValue=True, enableRowGroup=True)
    gb.configure_side_bar()
    gb.configure_grid_options(enableServerSideSorting=True, enableServerSideFilter=True)
    custom_css = {
    ".ag-row-hover": {"background-color": "orange !important","color":"orange !important"},
    ".ag-header-cell-label": {"color": "orange !important"},
    ".ag-header": {"background-color": "blue !important;"},
    ".ag-cell-value": {"color": "blue !important;"}
    }
    gb.configure_pagination(paginationAutoPageSize=100)
    #gb.configure_selection(selection_mode="multiple", use_checkbox=True)
    

    gridoptions = gb.build()

    response = AgGrid(
                    df,
                    height=400,
                    gridOptions=gridoptions,
                    enable_enterprise_modules=True,
                    update_mode=GridUpdateMode.SELECTION_CHANGED,
                    data_return_mode=DataReturnMode.FILTERED_AND_SORTED,
                    fit_columns_on_grid_load=False,
                    header_checkbox_selection_filtered_only=True,
                    custom_css=custom_css,
                    use_checkbox=True)

st.cache(ttl=60)
def to_excell(dff, chunk_size=1000000):
    num_chunks = math.ceil(len(dff) / chunk_size)
    output = BytesIO()
    writer = pd.ExcelWriter(output, engine='xlsxwriter')
    
    for i in range(num_chunks):
        start_idx = i * chunk_size
        end_idx = min((i + 1) * chunk_size, len(dff))
        sheet_name = f'Sheet {i+1}'
        dff_chunk = dff[start_idx:end_idx]
        
        dff_chunk.to_excel(writer, index=False, sheet_name=sheet_name)
        workbook = writer.book
        worksheet = writer.sheets[sheet_name]
        header_format = workbook.add_format({
            'bold': True
            # ,'bg_color': '#0066CC', # set the background color to blue
            # 'font_color': 'white' # set the font color to white
        })
        # Apply the format to the header row
        worksheet.set_row(0, None, header_format)
        format1 = workbook.add_format({'num_format': '0'})
        worksheet.set_column('A:A', None, format1)
        worksheet.set_column(0, 0, 10)
    
    writer.close()
    processed_data = output.getvalue()
    return processed_data
st.cache(ttl=60)
def to_excel(dff):
    output = BytesIO()
    writer = pd.ExcelWriter(output, engine='xlsxwriter')
    dff.to_excel(writer, index=False, sheet_name='Sheet1')
    workbook = writer.book
    worksheet = writer.sheets['Sheet1']
    header_format = workbook.add_format({
        'bold': True
        #,
        # 'bg_color': '#0066CC', # set the background color to blue
        # 'font_color': 'white' # set the font color to white
    })
    # Apply the format to the header row
    worksheet.set_row(0, None, header_format)
    format1 = workbook.add_format({'num_format': '0'})
    worksheet.set_column('A:A', None, format1)
    worksheet.set_column(0, 0, 10)
    writer.close()
    processed_data = output.getvalue()
    return processed_data
# def dataframee(df):
#     gb = GridOptionsBuilder.from_dataframe(df)
#     gb.configure_default_column(enablePivot=True, enableValue=True,groupable=True, value=True, enableRowGroup=True,autoHeaderHeight=True
#                                     #, aggFunc='sum'
#                                     #, editable=True
#                                     , filter=True, sortable=True)
#     gb.configure_side_bar()
#     gridoptions = gb.build()
#     response = AgGrid(df,height=600,columns_auto_size_mode=ColumnsAutoSizeMode.FIT_CONTENTS,gridOptions=gridoptions,enable_enterprise_modules=True,
#                       #data_return_mode=DataReturnMode.FILTERED_AND_SORTED,
#                           #fit_columns_on_grid_load=True,
#                           header_checkbox_selection_filtered_only=True,use_checkbox=True,allow_csv_export=True, preventDefaultOnFilter=False,  # enable CSV export
#     allow_excel_export=True)
st.cache(ttl=60)
def convert_df(df):
    result = df.to_csv(index=False, date_format='%Y/%m/%d %H:%M:%S', encoding='utf-8')
    return result

st.cache(ttl=60)
def to_excelbutton(result):
    output = BytesIO()
    writer = pd.ExcelWriter(output, engine='xlsxwriter')

    # Split data into multiple parts if the number of rows exceeds 1,000,000
    num_rows = result.shape[0]
    num_parts = math.ceil(num_rows / 1000000)
    for i in range(num_parts):
        start_idx = i * 1000000
        end_idx = min(start_idx + 1000000, num_rows)
        sheet_name = f'Sheet{i+1}'
        result_part = result.iloc[start_idx:end_idx]
        result_part.to_excel(writer, index=False, sheet_name=sheet_name)
        workbook = writer.book
        worksheet = writer.sheets[sheet_name]
        format1 = workbook.add_format({'num_format': '0'})
        worksheet.set_column('A:A', None, format1)
    
    # Save and retrieve processed data
    writer.save()
    processed_data = output.getvalue()
    return processed_data
#@st.cache(allow_output_mutation=True)
def getlast15date():
    endd_date = datetime.today().date()#.replace(hour=0,minute=0,second=0,microsecond=0)
    startt_date = datetime.today().date()-timedelta(days=15)#.replace(hour=0,minute=0,second=0,microsecond=0) - timedelta(days=15)
    return startt_date,endd_date
#@st.cache(allow_output_mutation=True)
def getlast30date():
    endd_date = datetime.today().date()
    startt_date = datetime.today().date() - timedelta(days=30)
    return startt_date,endd_date
#@st.cache(allow_output_mutation=True)
def getdate():
    start_day_of_this_month = datetime.today().replace(day=1,hour=0,minute=0,second=0,microsecond=0).date()
    last_day_of_prev_month = datetime.today().date()

            #start_day_of_prev_month = datetime.today().replace(    day=1,hour=0,minute=0,second=0,microsecond=0).date() - timedelta(days=last_day_of_prev_month.day)
    return start_day_of_this_month,last_day_of_prev_month
#@st.cache(allow_output_mutation=True)
def jan () :
        
                    #st.write(str(datetime.now().year)+'-01-01')
    start_date=datetime(datetime.now().year,1,1).date()
    end_date=datetime(datetime.now().year,2,1).date()
    return start_date,end_date    
#@st.cache(allow_output_mutation=True)
def feb():        
        
                    #st.write(str(datetime.now().year)+'-02-01')
    start_date=datetime(datetime.now().year,2,1).date() 
    end_date=datetime(datetime.now().year,3,1).date()
    return start_date,end_date
#@st.cache(allow_output_mutation=True)
def mar():    
        
                    #st.write(str(datetime.now().year)+'-03-01')
    start_date=datetime(datetime.now().year,3,1).date()
    end_date=datetime(datetime.now().year,4,1).date()
    return start_date,end_date
#@st.cache(allow_output_mutation=True)
def apr():    
        
                    #st.write(str(datetime.now().year)+'-04-01')
    start_date=datetime(datetime.now().year,4,1).date() 
    end_date=datetime(datetime.now().year,5,1).date()
    return start_date,end_date
#@st.cache(allow_output_mutation=True)
def may():    
        
                    #st.write(str(datetime.now().year)+'-05-01')
    start_date=datetime(datetime.now().year,5,1).date()
    end_date=datetime(datetime.now().year,6,1).date()
    return start_date,end_date
#@st.cache(allow_output_mutation=True)
def jun():
       
                    #st.write(str(datetime.now().year)+'-06-01')
    start_date=datetime(datetime.now().year,6,1).date()
    end_date=datetime(datetime.now().year,7,1).date()
    return start_date,end_date
#@st.cache(allow_output_mutation=True)
def jul():    
       
                    #st.write(str(datetime.now().year)+'-07-01')
    start_date=datetime(datetime.now().year,7,1).date()
    end_date=datetime(datetime.now().year,8,1).date()
    return start_date,end_date
#@st.cache(allow_output_mutation=True)
def avg():    
        
                    #st.write(str(datetime.now().year)+'-08-01')
    start_date=datetime(datetime.now().year,8,1).date() 
    end_date=datetime(datetime.now().year,9,1).date()
    return start_date,end_date
#@st.cache(allow_output_mutation=True)
def sep():    
        
                    #st.write(str(datetime.now().year)+'-09-01')
    start_date=datetime(datetime.now().year,9,1).date() 
    end_date=datetime(datetime.now().year,10,1).date()
    return start_date,end_date
#@st.cache(allow_output_mutation=True)
def oct():    
        
            #st.write(str(datetime.now().year)+'-01-01')
    start_date=datetime(datetime.now().year,10,1).date()
    end_date=datetime(datetime.now().year,11,1).date()
    return start_date,end_date
#@st.cache(allow_output_mutation=True)
def nov():
       
            # st.write(str(datetime.now().year)+'-01-01')
    start_date=datetime(datetime.now().year,11,1).date()
    end_date=datetime(datetime.now().year,12,1).date()
    return start_date,end_date
#@st.cache(allow_output_mutation=True)
def dec():
      
            #         st.write(str(datetime.now().year)+'-01-01')
    start_date=datetime(datetime.now().year,12,1).date()
    end_date=datetime(datetime.now().year+1,1,1).date()
    return start_date,end_date

#@st.cache(allow_output_mutation=True)
def twenty(): 
            #         st.write(str(datetime.now().year)+'-01-01')
    start_date=datetime(2020,1,1).date()
    end_date=datetime(2021,1,1).date()
    return start_date,end_date
#@st.cache(allow_output_mutation=True)
def twentyone(): 
            #         st.write(str(datetime.now().year)+'-01-01')
    start_date=datetime(2021,1,1).date()
    end_date=datetime(2022,1,1).date()
    return start_date,end_date
#@st.cache(allow_output_mutation=True)
def twentytwo(): 
            #         st.write(str(datetime.now().year)+'-01-01')
    start_date=datetime(2022,1,1).date()
    end_date=datetime(2023,1,1).date()
    return start_date,end_date
#@st.cache(allow_output_mutation=True)
def twentythree(): 
            #         st.write(str(datetime.now().year)+'-01-01')
    start_date=datetime(2023,1,1).date()
    end_date=datetime(2024,1,1).date()
    return start_date,end_date
