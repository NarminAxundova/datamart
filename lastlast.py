import pandas as pd
import streamlit as st
import psycopg2
import streamlit.components.v1 as components
from PIL import Image
from yaml.loader import SafeLoader
import csv
import json
import yaml
import os
import pyexlatex as pl
from st_aggrid import AgGrid, GridOptionsBuilder, DataReturnMode,ColumnsAutoSizeMode,GridUpdateMode
import numpy as np
import streamlit_authenticator as stauth
from datetime import datetime,timedelta
from streamlit_option_menu import option_menu
from config import config
import hydralit_components as hc
st.set_page_config(
            page_title="E-datamart",
            layout="wide",
            initial_sidebar_state="expanded",
            page_icon=Image.open('emanatpng.png')
        )

import funcc
converted = str()
dictionary=config()
# iterating over dictionary using a for loop
for key in dictionary:
    converted += key + "= '" + dictionary[key] + "' "
converted=converted[:len(converted)-1]
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
@connect
@st.cache(allow_output_mutation=True,ttl=60*60,hash_funcs={psycopg2.extensions.connection: id})
def all_transactions(conn,column_choice,scripts,sdas,mp_pr,mp_sr,pointmpay,pointmod,status):
    c=f'''select {column_choice} from (SELECT id_operation,id_point,replace ( "Provider yeni adlar",'[MP] ','' ) "Provider", "Service name yeni adlar" "Service", time_server, time_process,  account, account2,  case when state =60 and substate=0 then 'Success'
when state=60 and substate=1 then 'Success/Cancelled '
when state=60 and substate=2 then 'Success/Funds return '
when state=60 and substate=3 then 'Success/Corrected '
when state=60 and substate=4 then 'Success/Blocked '
when state=80 and substate=1 then 'Error/Cancelled by user'
when state=80 and substate=3 then 'Error/Cancelled by support'
when state=80 and substate=5 then 'Error/Rejected by provider'
when state=80 and substate=6 then 'Error/Corrected'
when state=80 and substate=7 then 'Error/Uncorrectable error'
when state=80 and substate=8 then 'Error/Revocated'
when state=80 and substate=12 then 'Error/Blocked by user'
when state=40 and substate=2 then 'Processing/Provider determined'
when state=40 and substate=3 then 'Processing/Processing'
when state=40 and substate=5 then 'Processing/Unknown'
when state=40 and substate=6 then 'Processing/Picked up'
when state=40 and substate=7 then 'Processing/Error'
when state=40 and substate=8 then 'Processing/In wait'
when state=40 and substate=9 then 'Processing/Confirm'
else 'Unknown'
end status, cast(sum_income as numeric)/100 sum_income , 
cast(sum_outcome as numeric)/100 sum_outcome , "comment", id_money_collection, collection_time,
COALESCE(cashcount, 0) AS cashcount,
    COALESCE(bir, 0) AS "1M",
    COALESCE(bes, 0) AS "5M",
    COALESCE("on", 0) AS "10M",
    COALESCE(iyirmi, 0) AS "20M",
    COALESCE(elli, 0) AS "50M",
    COALESCE(yuz, 0) AS "100M",
    COALESCE(ikiyuz, 0) AS "200M",provider_trans,'mpay' company
FROM "e-datamart-F".data_mart_mpay where "Provider yeni adlar" ilike '%[mp]%' and ({scripts})   {mp_pr} {mp_sr}   {pointmpay}
union all
SELECT paymentid, pointid,replace ( "Provider yeni adlar",'[MOD] ','' ), "Service name yeni adlar",    createtime, statustime, "Number",'',case when status =2 then 'Success'
when status=3 then 'Revoked'
else 'Processing'
end  statuss, payvalue, servicevalue,
coalesce("Comment",portalcomment), cashoutid, cashoutdate,   cashcount, bir, bes, "on", iyirmi, elli, yuz, ikiyuz, transactionid,'modenis' company
FROM "e-datamart-F".data_mart_modenis where "Provider yeni adlar" ilike '%[MOD]%' and ( {sdas} )   {mp_pr} {mp_sr} {pointmod})op  '''
    d= f'''     {status}'''
    sql=c+d
    #st.write(sql)
    #SQl_query_filtered=pd.read_sql(sql,conn)
    cur = conn.cursor()
    cur.execute(sql)
    SQl_query_filtered = cur.fetchall()
    df_filtered = pd.DataFrame.from_records(SQl_query_filtered, columns=[x[0] for x in cur.description])
    return df_filtered
@st.cache(allow_output_mutation=True,ttl=60*60,hash_funcs={psycopg2.extensions.connection: id})
def all_transactions_group(conn,dd,a20,a21,a22,a23,ddd,a200,a211,a222,a233,mp_pr,mp_sr,status):
    s=f'''select * from (sELECT replace ( "Provider yeni adlar",'[MP] ','' ) "Provider", "Service name yeni adlar" "Service",   
to_char(time_server,'Month') "month",extract(year from time_server) "İl",
case when state =60 and substate=0 then 'Success'
when state=60 and substate=1 then 'Success/Cancelled'
when state=60 and substate=2 then 'Success/Funds return'
when state=60 and substate=3 then 'Success/Corrected'
when state=60 and substate=4 then 'Success/Blocked'
when state=80 and substate=1 then 'Error/Cancelled by user'
when state=80 and substate=3 then 'Error/Cancelled by support'
when state=80 and substate=5 then 'Error/Rejected by provider'
when state=80 and substate=6 then 'Error/Corrected'
when state=80 and substate=7 then 'Error/Uncorrectable error'
when state=80 and substate=8 then 'Error/Revocated'
when state=80 and substate=12 then 'Error/Blocked by user'
when state=40 and substate=2 then 'Processing/Provider determined'
when state=40 and substate=3 then 'Processing/Processing'
when state=40 and substate=5 then 'Processing/Unknown'
when state=40 and substate=6 then 'Processing/Picked up'
when state=40 and substate=7 then 'Processing/Error'
when state=40 and substate=8 then 'Processing/In wait'
when state=40 and substate=9 then 'Processing/Confirm'
else 'Unknown'
end status, sum(cast(sum_income as numeric)/100) sum_income , 
sum(cast(sum_outcome as numeric)/100) sum_outcome ,'mpay' company
FROM "e-datamart-F".data_mart_mpay where "Provider yeni adlar" ilike '%[mp]%' and ((  {dd} or {a20} or {a21} or {a22} or {a23}))   {mp_pr} {mp_sr} 
group by "Provider","Service",status,company,"month","İl"
union all
SELECT replace ( "Provider yeni adlar",'[MOD] ','' ) "Provider", "Service name yeni adlar" "Service",    
to_char(createtime,'Month') "month",extract(year from createtime) "İl"
,case when status =2 then 'Success'
when status=3 then 'Revoked'
else 'Processing'
end  status, sum(payvalue), sum(servicevalue),
'modenis' company
FROM "e-datamart-F".data_mart_modenis where "Provider yeni adlar" ilike '%[MOD]%' and ( (  {ddd} or {a200} or {a211} or {a222} or {a233}) )  {mp_pr} {mp_sr} 
group by  "Provider","Service",status,company,"month","İl") f  where  {status}'''
    # st.write(s)
    cur = conn.cursor()
    cur.execute(s)
    SQl_query_filtered = cur.fetchall()
    df_filtered = pd.DataFrame.from_records(SQl_query_filtered, columns=[x[0] for x in cur.description])
    return df_filtered
@st.cache(allow_output_mutation=True,ttl=60*60,hash_funcs={psycopg2.extensions.connection: id})
def mpay_payment_checks(conn,column_choice,scripts,mp_pr,mp_sr,mpay_point,status,mpay_ccount):
    c=f'''select {column_choice} from (SELECT id_operation, "Provider", "Service", "Agent", provider_trans, g.id_point, account, account2, status, time_server, time_process, sum_income, sum_outcome, g.cashcount, "1M", "5M", "10M", "20M", "50M", "100M", "200M", op.id_money_collection, op.collection_time, "comment" ,
cast(op.cashcount as varchar) "TotalCashCount" ,cast(op.cashamount  as varchar) "TotalAmount" 
FROM "e-datamart-F".vw_dm_mpay g left join "e-datamart-F".data_mart_mpay_inkassasiya op on op.id_money_collection=g.id_money_collection and op.id_point=g.id_point '''
    d=f'''where ( {scripts} )   { mp_pr}    {mp_sr}    {mpay_point}   {status}             {mpay_ccount} ) ui            '''
    sql=c+d
    st.write(sql)
    #SQl_query_filtered=pd.read_sql(sql,conn)
    cur = conn.cursor()
    cur.execute(sql)
    SQl_query_filtered = cur.fetchall()
    df_filtered = pd.DataFrame.from_records(SQl_query_filtered, columns=[x[0] for x in cur.description])
    df_filtered.sum_outcome=df_filtered.sum_outcome/100
    df_filtered.sum_income=df_filtered.sum_income/100
    return df_filtered


   
@st.cache(allow_output_mutation=True,ttl=60*60,hash_funcs={psycopg2.extensions.connection: id})
def mpay_payment_check_group(conn,dd,a20,a21,a22,a23,mpay_pr,mpay_serv,status):
    c='''select * from (SELECT   "Provider yeni adlar",
       "Service name yeni adlar"
,  case when state =60 and substate=0 then 'Success'
when state=60 and substate=1 then 'Success/Cancelled '
when state=60 and substate=2 then 'Success/Funds return '
when state=60 and substate=3 then 'Success/Corrected '
when state=60 and substate=4 then 'Success/Blocked '
when state=80 and substate=1 then 'Error/Cancelled by user'
when state=80 and substate=3 then 'Error/Cancelled by support'
when state=80 and substate=5 then 'Error/Rejected by provider'
when state=80 and substate=6 then 'Error/Corrected'
when state=80 and substate=7 then 'Error/Uncorrectable error'
when state=80 and substate=8 then 'Error/Revocated'
when state=80 and substate=12 then 'Error/Blocked by user'
when state=40 and substate=2 then 'Processing/Provider determined'
when state=40 and substate=3 then 'Processing/Processing'
when state=40 and substate=5 then 'Processing/Unknown'
when state=40 and substate=6 then 'Processing/Picked up'
when state=40 and substate=7 then 'Processing/Error'
when state=40 and substate=8 then 'Processing/In wait'
when state=40 and substate=9 then 'Processing/Confirm'
else 'Unknown'
end status,to_char(time_server,'Month') "month",extract(year from time_server) "İl",sum( cast(sum_income as numeric)/100) sum_income,count(*),round(avg(cast(sum_income as numeric)/100),2) "avg"
FROM "e-datamart-F".data_mart_mpay 
'''
    d=f''' where (  {dd} or {a20} or {a21} or {a22} or {a23})   { mpay_pr}    {mpay_serv}                          '''
    a=f''' group by "Provider yeni adlar",
       "Service name yeni adlar",status,"month","İl") h  {status}'''
    sql=c+d+a
    #st.write(sql)
    SQl_query_filtered=pd.read_sql(sql,conn)

 
    df_filtered = pd.DataFrame(SQl_query_filtered)    
    return df_filtered

@st.cache(allow_output_mutation=True,ttl=60*60,hash_funcs={psycopg2.extensions.connection: id})
def modenis_paymentcheck(conn,servi_mod,point_mod,scripts,status,mod_pr,mod_ccount):
    a=f'''SELECT paymentid, m.pointid, "Provider", "Service", providerpaymentid, providerpaymentidstring, transactionid, createtime, statustime, "Number", status, topaymentid, "Comment", portalcomment, m.cashoutid, m.cashoutdate, payvalue, servicevalue, providerprofit, providerprofitvalue, m.cashcount, bir, bes, "on", iyirmi, elli, yuz, ikiyuz,t.cashoutunitscount ,t.cashoutunitssum 
FROM "e-datamart-F".vw_dm_modenis m left join "e-datamart-F"."data_mart_main.terminalcashout" t on m.cashoutid =t.cashoutid and m.pointid =t.pointid 
where (  {scripts}  )  {servi_mod}   {point_mod}  {mod_ccount} '''
    d=f'''   '''
    c=f''' {status} {mod_pr}  '''
    sql=a+d+c
    #st.write(sql)
    SQl_query_filtered=pd.read_sql(sql,conn)

    df_filtered = pd.DataFrame(SQl_query_filtered) 
    return df_filtered
@st.cache(allow_output_mutation=True,ttl=60*60,hash_funcs={psycopg2.extensions.connection: id})
def mpay_terminal_group(conn,dd,a20,a21,a22,a23):
    sql=f'''select cast(s.id_service as varchar) ServiceID,s.Name ServiceName,to_char(time_server,'Month') "Month" ,extract (year from time_server) "İl"
    ,coalesce (count(m.id_operation) ,0) TotalCount,
                                coalesce (sum(cast(m.sum_income as numeric) / 100),0) TotalAmount,
                                coalesce (count(m.id_operation) filter (where m.state<>60 and substate<>0),0) RejectCount,
                                coalesce (sum(cast(m.sum_income as numeric) / 100) filter (where m.state<>60 and substate<>0),0) RejectAmount
                                from
                                        "work".master m
                                left join "work".services s on
                                        m.id_service = s.id_service
                                where ({dd} or {a20} or {a21} or {a22} or {a23}) 
                                and m.id_point not in (6325,6320,8,6322)
                                        group by s.id_service, s."name","Month","İl"
                            '''
    SQl_query_filtered = pd.read_sql(sql, conn) 
    #SQl_query_filtered['gun']=SQl_query_filtered['gun'].dt.strftime('%Y-%m-%d')
    df_filtered = pd.DataFrame(SQl_query_filtered)
    return df_filtered


@st.cache(allow_output_mutation=True,ttl=60*60,hash_funcs={psycopg2.extensions.connection: id})
def mpay_terminal(conn,scripts):
    sql=f'''select
                                s.id_service ServiceID,
                                s.Name ServiceName,
                                coalesce (count(m.id_operation) ,0) TotalCount,
                                coalesce (sum(cast(m.sum_income as numeric) / 100),0) TotalAmount,
                                coalesce (count(m.id_operation) filter (where m.state<>60 and substate<>0),0) RejectCount,
                                coalesce (sum(cast(m.sum_income as numeric) / 100) filter (where m.state<>60 and substate<>0),0) RejectAmount,cast(cast(time_server as date) as varchar) gun
                                from
                                        "work".master m
                                left join "work".services s on
                                        m.id_service = s.id_service
                                where
                                        {scripts} 
                                        and m.id_point not in (6325,6320,8,6322)
                                        group by s.id_service, s."name",gun
                            '''
    SQl_query_filtered = pd.read_sql(sql, conn) 
    #SQl_query_filtered['gun']=SQl_query_filtered['gun'].dt.strftime('%Y-%m-%d')
    df_filtered = pd.DataFrame(SQl_query_filtered)
    return df_filtered
@st.cache(allow_output_mutation=True,ttl=60*60,hash_funcs={psycopg2.extensions.connection: id})
def modenis_terminal(conn,scripts):
    sql=f'''select s.osmpproviderid,s."Name" ,coalesce (count(*) ,0) "PayCount",coalesce (sum(payvalue) ,0) "PayAmount",
                        coalesce (SUM(PayValue- ServiceValue) + SUM(ProviderProfitValue)  filter (where status=2),0) "Profit",coalesce (sum(payvalue) filter (where status<>2),0) "RejectAmount",
                        coalesce (count(*) filter (where status<>2),0) "RejectCount",cast(cast(createtime as date) as varchar) gun
                        FROM main.payment p join main.service s on p.serviceid =s.serviceid 
                        where {scripts}  and PointID not in (4702,4244,4245,3136) group by s.osmpproviderid,s."Name",gun
                        '''
    SQl_query_filtered = pd.read_sql(sql, conn)
    df_filtered = pd.DataFrame(SQl_query_filtered)
    return df_filtered
@st.cache(allow_output_mutation=True,ttl=60*60,hash_funcs={psycopg2.extensions.connection: id})
def  modenis_terminal_group_check(conn,dd,a20,a21,a22,a23):
    a='''select	s.osmpproviderid,	s."Name" ,	to_char(createtime , 'Month') "Month",
    extract(year from createtime ) "İl",	coalesce (count(*) ,	0) "PayCount",
    coalesce (sum(payvalue) ,	0) "PayAmount",	coalesce (SUM(PayValue- ServiceValue) + SUM(ProviderProfitValue) filter (	where status = 2),	0) "Profit",
    coalesce (sum(payvalue) filter (	where status <> 2),	0) "RejectAmount",	coalesce (count(*) filter (	where status <> 2),	0) "RejectCount"
from	main.payment p join main.service s on
    p.serviceid = s.serviceid '''
    d=f''' where (  {dd} or {a20} or {a21} or {a22} or {a23} )'''
    c=''' and PointID not in (4702,4244,4245,3136) 
                        group by s.osmpproviderid,s."Name","Month","İl"'''
    sql=a+d+c
    #st.write(sql)
    SQl_query_filtered=pd.read_sql(sql,conn)

    df_filtered = pd.DataFrame(SQl_query_filtered)
    return df_filtered

@st.cache(allow_output_mutation=True,ttl=60*60,hash_funcs={psycopg2.extensions.connection: id})
def abb(conn,scripts):
    sql=f'''SELECT company, "Service name", "Date"  "Tarix", "sum income" sum_income, "sum outcome" sum_outcome, "Terminal ID", address, "Customer name"
                            FROM "e-datamart-F".abb2 where {scripts} '''
    SQl_query_filtered = pd.read_sql(sql, conn)
    #SQl_query_filtered['Date']=SQl_query_filtered['Date'].dt.strftime('%Y-%m-%d %H:%M:%S')
    SQl_query_filtered['Tarix']=pd.to_datetime(SQl_query_filtered.Tarix, format='%Y-%m-%d %H:%M:%S')
    df_filtered = pd.DataFrame(SQl_query_filtered)
    return df_filtered
@st.cache(allow_output_mutation=True,ttl=24*60*60,hash_funcs={psycopg2.extensions.connection: id})
def abb_group(conn,dd,a20,a21,a22,a23):
    sql=f'''SELECT company, "Service name",to_char( "Date",'Month') "Month",extract(year from "Date") "İl", sum("sum income")  sum_income,count(*)
FROM "e-datamart-F".abb2 where   (  {dd} or {a20} or {a21} or {a22} or {a23} )
group by company, "Service name","Month","İl" '''
    SQl_query_filtered = pd.read_sql(sql, conn)
    df_filtered = pd.DataFrame(SQl_query_filtered)
    return df_filtered

@st.cache(allow_output_mutation=True,ttl=60*60,hash_funcs={psycopg2.extensions.connection: id})
def modenis_group_payment_check(conn,dd,a20,a21,a22,a23,servi_mod,mod_pr,status):
    a='''select  "Provider yeni adlar", "Service name yeni adlar",statuss, "month","İl",sum(payvalue) payvalue,count(*),round(avg(payvalue),2) "avg"
from (SELECT   "Provider yeni adlar", "Service name yeni adlar",  case when status =2 then 'Success'
when status=3 then 'Revoked'
else 'Processing'
end statuss, to_char(createtime,'Month') "month", payvalue,extract(year from createtime ) "İl"
FROM "e-datamart-F".data_mart_modenis'''
    b=f''' where ( {dd} or {a20} or {a21} or {a22} or {a23} )  {servi_mod} ) f  where statuss in  {status} {mod_pr}  
    group by  "month",   "Provider yeni adlar", "Service name yeni adlar",statuss,"İl"'''
    sql =a+b
    #st.write(sql)
    SQl_query_filtered = pd.read_sql(sql,conn)


    df_filtered = pd.DataFrame(SQl_query_filtered) 
    return df_filtered

#def mpay_collection(conn):
    
def dfg(conn):
    ser=pd.read_sql("""SELECT replace ( replace ( "Provider yeni adlar",'[MP] ','' ),'[MOD] ','' ) "Provider yeni adlar", "Service name yeni adlar", osmp, "Service ID", worksercervideid, active
FROM "data".modenis_servis where active=True""",conn,index_col=None)
    point=pd.read_sql("""select distinct pointid from (select pointid from "e-datamart-F".data_mart_main_point
union all
select id_point from "e-datamart-F".data_mart_work_points)d order by 1""",conn,index_col=None)
    return ser,point
def getdata(conn):
    providers_mp = pd.read_sql("""SELECT distinct id_provider,  providername FROM "e-datamart-F".data_mart_mpay_provider_service order by 2""",conn, index_col=None)
    point_mp=pd.read_sql("""SELECT id_point, "name", address FROM "e-datamart-F".data_mart_work_points where "blocked" =false order by 1""",conn,index_col=None)
    services_mp=pd.read_sql("""SELECT  id_provider, id_service, servicename FROM "e-datamart-F".data_mart_mpay_provider_service order by 3""",conn, index_col=None)
    provider_mod=pd.read_sql("""SELECT nomenclatureid, nomenclaturename FROM "e-datamart-F".data_mart_main_nomenclature union all select 20000,'Digər' order by 2""",conn,index_col=None)
    point_mod=pd.read_sql("""SELECT distinct pointid,  pointname, pointaddress, enabled FROM "e-datamart-F".data_mart_main_point order by 1""",conn,index_col=None)
    services_mod=pd.read_sql("""SELECT serviceid, "Name", coalesce (s.nomenclatureid,20000) providerid  
FROM "e-datamart-F".data_mart_main_service s left join "e-datamart-F".data_mart_main_nomenclature n on s.nomenclatureid =n.nomenclatureid  order by 2""",conn,index_col=None)
    servicename=pd.read_sql("""select "Provider yeni adlar", "Service name yeni adlar", osmp, "Service ID", worksercervideid, active
FROM "data".modenis_servis where active=True""",conn,index_col=None)
   # mpay_collection=pd.read_sql("""SELECT id_money_collection FROM "e-datamart-F".data_mart_mpay_money_collection where collection_time between current_date -interval '2 month' and current_date""",conn,index_col=None)
    return providers_mp,services_mp,point_mp,provider_mod,services_mod,point_mod,servicename#,mpay_collection
@connect

def main(conn):
    fpath = os.path.join('db', 'configg.yaml')
    with open(fpath) as file:
        config = yaml.load(file, Loader=stauth.SafeLoader)

    authenticator = stauth.Authenticate(
                config['credentials'],
                config['cookie']['name'],
                config['cookie']['key'],
                config['cookie']['expiry_days'],
                config['preauthorized']
            )

    c1, c2, c3 = st.columns([1, 1, 1])
    with c2:
        st.write(' ')
        name, authentication_status, username = authenticator.login(
                    'Login', 'main')
        if authentication_status == False:
                        st.error('Username/password is incorrect')
        elif authentication_status == None:
                        st.warning('Please enter your username and password')
    d1, d2, d3 = st.columns([1, 6, 1])
    with d2:
        if authentication_status:
            user = st.session_state['name']
            username = st.session_state['username']
            fpathz = os.path.join('db', 'roless.csv')
            roles = pd.read_csv(fpathz)
            try:
                user_role = roles['role'][(roles['user'] == username)]
                
                if user_role.iloc[0] == 'user':
                    menu_data = [
                                        
                                        {'icon': "fa fa-link", 'label': 'Maliyyə'},
                                        {'icon': "fa fa-table" ,'label': 'Əməliyyatlar'}
                                        
                                ]
                elif user_role.iloc[0] == 'marketing':
                    menu_data = [   
                                
                                {'icon': "fa fa-download", 'label': 'Hazır hesabatlar'}
                                
                                ]
                else:
                    menu_data = [       
                                        {'icon': "fa fa-link",
                                            'label': 'Maliyyə'},
                                        {'icon': "fa fa-download",
                                            'label': 'Hazır hesabatlar'},
                                        {'icon': "fa fa-table", 'label': 'Əməliyyatlar'}
                                        ]
            except:
                menu_data = [   
                                {'icon': "fa fa-link", 'label': 'Maliyyə'},
                                {'icon': "fa fa-download", 'label': 'Hazır hesabatlar'},
                                {'icon': "fa fa-table", 'label': 'Əməliyyatlar'}
                                ]
            over_theme = {'txc_inactive': 'blue','menu_background':'orange','txc_active':'blue','option_active':'white'}
            selected3 = hc.nav_bar(
                            menu_definition=menu_data, home_name=' ',  login_name=user, sticky_mode='sticky',override_theme=over_theme,)
            if selected3 == '{}'.format(user):
                authenticator.logout('Logout', 'main')
                            
                #st.write(f'Xoş  gəldin, *{st.session_state["name"]}*')
                username = st.session_state["username"]
                if username == 'superadmin':
                    fpath = os.path.join('db', 'configg.yaml')
                    with open(fpath, 'r') as f:
                        config = yaml.safe_load(f)
                    
                    # Extract the usernames
                    usernames = [user for user in config.get('credentials', {}).get('usernames', {})]

                    #st.write(usernames)
                    # Create a selectbox for the usernames
                    
                    inf = st.checkbox('Get info about roles')
                    if inf:
                        st.info("""superadmin - Includes all tabs and adding roles;
                                                admin - Include all tabs;
                                                user - Includes Bulk Search, Status Check and Compare modules""")
                    with st.form("Set Roles"):
                        st.header('Set Roles')
                        roles = ['superadmin','admin','user','marketing']
                        users = ['finance','mashurova','muradmammadov','recon','sevinjirzayeva','test1']
                        fpatz = os.path.join('db', 'roless.csv')
                        roles_json = pd.read_csv(fpatz)
                                    
                        user = st.selectbox('Users',usernames)
                        roless = st.selectbox('Role',roles)
                        set_role = st.form_submit_button('Set Role')
                        
                        if set_role:
                            roles_json['role'][(roles_json['user']==user)] = roless
                            roles_json.to_csv(fpatz)
                        
                                

                try:
                    if authenticator.reset_password(username, 'Reset password'):
                                            
                        st.success('Password modified successfully')
                        fpath = os.path.join('db', 'configg.yaml')
                        with open(fpath, 'w') as file:
                            yaml.dump(config, file, default_flow_style=False)
                except Exception as e:
                    st.error(e)

                try:
                    if authenticator.update_user_details(username, 'Update user details'):
                        st.success('Entries updated successfully')
                        fpath = os.path.join('db', 'configg.yaml')
                        with open(fpath, 'w') as file:
                            yaml.dump(config, file, default_flow_style=False)
                except Exception as e:
                    st.error(e)

                try:
                    if authenticator.register_user('Register user', preauthorization=False):
                        st.success('User registered successfully')
                        fpath = os.path.join('db', 'configg.yaml')
                        with open(fpath, 'w') as file:
                            yaml.dump(config, file, default_flow_style=False)
                        role_csv_path = os.path.join('db', 'roless.csv')

                        # Get the username of the registered user
                        username = authenticator.username
                        st.write(username)

                        # Define the roles for the new user
                        new_user_roles = 'user'

                        # Write the new roles to the role.csv file
                        with open(role_csv_path, mode='a', newline='') as file:
                            writer = csv.writer(file)
                            for role in new_user_roles:
                                writer.writerow([username, role])
                except Exception as e:
                    st.error(e)
                        

                        

                                    #hc.info_card(title='Table Updated', content='Done!', sentiment='good',bar_value=89)
            
            elif selected3==' ': 
                st.write(f'Xoş  gəldin, *{st.session_state["name"]}*')
                st.markdown(
    """
    <style>
        div [data-testid=stImage]{
            text-align: center;
            display: block;
            margin-left: auto;
            margin-right: auto;
            width: 100%;
            margin-top: 20%;
        }

    </style>
    """, unsafe_allow_html=True
)
                icon=Image.open('edatamart.png')        
                st.image(icon)
            elif selected3=="Əməliyyatlar":
                company=st.sidebar.radio("",("Mpay","Modenis","All"),horizontal=True)
                
                
                
                if  company=='Modenis': 
                    timepar=' createtime ' 
                    collection_time=' cashoutdate '
                else :
                    
                    timepar=' time_server '
                    collection_time=' g.collection_time '
                
                 
                
                #b=st.checkbox('Report')
                allmodeniss_container =st.container() 
                allmpayy_container =st.container() 
                pr,sr,pointid_mp,pr_mod,ser_mod,point_mod,servicename=getdata(conn) 
                servicenamemod=servicename[servicename['Service ID'].notnull()]  
                #st.write(mpay_coll)
                provider_name = pr["providername"]
                service_name_mpay=sr["servicename"]
                pointid_mpay=pointid_mp["id_point"]
                prid_mpay=pr["id_provider"] 
                provider_name_mod = pr_mod["nomenclaturename"]
                
                point_mod=point_mod["pointid"] 
                current_year=datetime.now().year
                next_year=datetime.now().year+1
                current_month=datetime.now().month
                servicename=servicename[servicename['worksercervideid'].notnull()]
                prrname=np.unique(servicename["Provider yeni adlar"])
                
                     
                if company=='Mpay':
                   if allmpayy_container:    
                        
                        prras = st.sidebar.multiselect('Provider', prrname,help='Bütün providerləri seçmək üçün heç bir seçim etməyin')
                        if prras !=[]:
                            
                            prname=servicename[servicename["Provider yeni adlar"].isin(prras)]
                            #st.write(prname)
                            if len(prras)==1:
                                prras     = tuple(prras)
                                prras     = "('{}')".format(prras    [0]) 
                                mp_pr=f' and "Provider" in {prras}'
                                
                            elif len(prras)>1 :
                                mp_pr=f' and "Provider" in {tuple(prras)}'
                            
                        else :
                            prname=servicename
                            mp_pr=' '
                        sr_name=np.unique(prname["Service name yeni adlar"])
                        serv = st.sidebar.multiselect('Xidmət',sr_name)
                        if serv==[]:
                            serv=sr_name
                            mp_sr=' '
                        elif len(serv)==1:
                            serv     = tuple(serv)
                            serv     = "('{}')".format(serv    [0]) 
                            mp_sr=f' and "Service" in {serv} '
                        else:
                            mp_sr=f' and "Service" in {tuple(serv)} '
                        # st.write(mp_pr)
                        # st.write(mp_sr)
                                    
                        stt=('Success', 'Success/Cancelled ','Success/Funds return ', 'Success/Corrected ', 'Success/Blocked ', 'Error/Cancelled by user', 'Error/Cancelled by support', 'Error/Rejected by provider', 'Error/Corrected', 'Error/Uncorrectable error', 'Error/Revocated', 'Error/Blocked by user', 'Processing/Provider determined', 'Processing/Processing', 'Processing/Unknown', 'Processing/Picked up', 'Processing/Error', 'Processing/In wait','Processing/Confirm' )
                        status=st.sidebar.multiselect("Status",stt,default="Success",help='Bütün statusları seçmək üçün heç bir seçim etməyin')
                        status=tuple(status)
                        if status ==():
                            status=stt
                            status=' '
                                #st.write(status)
                                #new_row = {'id_point': 'All'}
                        elif len(status)>1:
                            status = status
                            status=f' and status in {status} '
                        elif len(status)==1:
                            status = tuple(status)
                            status = "('{}')".format(status[0])
                            status=f' and status in {status} '
                        new_row=pd.Series(['All'])
                            #pointidd_mpay = pointid_mpay.append(new_row, ignore_index=True)
                        mpay_chashout=st.sidebar.text_input('İnkassasiya id')
                        if mpay_chashout!='':
                            mpay_ccount=f' and g.id_money_collection in ({mpay_chashout})'
                        else:
                            mpay_ccount=' '
                        pointidd_mpay=pd.concat([pointid_mpay,new_row])
                        pointmpay=st.sidebar.multiselect("Pointid",pointidd_mpay,default='All')
                        pointmpay=tuple(pointmpay)
                        if len(pointmpay)>1:
                            pointmpay = pointmpay
                        else:
                            pointmpay = tuple(pointmpay)
                            pointmpay = "('{}')".format(pointmpay[0])               
                        if 'All' in pointmpay:
                            mpay_point= ' '
                                        
                        else : 
                            mpay_point= f' and id_point in {pointmpay}  '
                        st.sidebar.header("Tarixi seçin:")
                        date_typee=st.sidebar.radio("",("Ödəniş tarixi","İnkassasiya tarixi"),horizontal=True)
                        if date_typee=="Ödəniş tarixi":
                            date_type=' time_server '
                        else:
                            date_type=' g.collection_time '
                        a,b=st.sidebar.columns(2)     
                        
                        v=datetime.today().replace(day=1)
                        div=st.sidebar.container()
                        full_script=''
                        start_date=a.date_input("Başlanğıc tarix",v)
                        end_date=b.date_input("Son tarix")
                        f=a.checkbox("Filter")
                        st.sidebar.write(' ')
                        st.sidebar.write(' ')
                        c,d=st.sidebar.columns(2)
                        s2020=c.checkbox("2020")
                        s2021=d.checkbox("2021")
                        s2022=c.checkbox("2022")
                        s2023=d.checkbox("2023")
                        a=('Yanvar','Fevral','Mart','Aprel','May','İyun','İyul','Avqust','Sentyabr','Oktyabr','Noyabr','Dekabr')
                        full_script = '' 
                        if f:
                            start_date=start_date
                            end_date=end_date
                            ccc=f'''  {date_type} between '{start_date}' and '{end_date}' '''
                            if full_script:
                                full_script += ' or '
                            full_script += ccc
                        if s2020:
                            year=2020
                            ss2020=st.sidebar.multiselect('2020',a)
                        if s2021:
                            year=2021
                            ss2021=st.sidebar.multiselect('2021 ',a)
                        if s2022:
                            year=2022
                            ss2022=st.sidebar.multiselect('2022 ',a)
                        if s2023:
                            year=2023
                            ss2023=st.sidebar.multiselect('2023 ',a)
                        if s2020:
                            if ss2020!= ' ':
                                year=2020
                                #full_script = ''   
                                if 'Yanvar' in ss2020:
                                    jan_script=f'''  {date_type} between \'{year}-01-01\' and \'{year}-02-01\' '''
                                    if full_script:
                                        full_script += ' or '
                                    full_script += jan_script
                                else :
                                    jan_script=' '
                                if 'Fevral' in ss2020:
                                    feb_script=f'''  {date_type} between \'{year}-02-01\' and \'{year}-03-01\' '''
                                    if full_script:
                                        full_script += ' or '
                                    full_script += feb_script
                                else :
                                    feb_script=' '
                                if 'Mart' in ss2020:
                                    if full_script:
                                        full_script += ' or '
                                    mar_script=f'''  {date_type} between \'{year}-03-01\' and \'{year}-04-01\' '''
                                    full_script += mar_script
                                else :
                                    mar_script=' '
                                if 'Aprel' in ss2020:
                                    if full_script:
                                        full_script += ' or '
                                    apr_script=f'''  {date_type} between \'{year}-04-01\' and \'{year}-05-01\' '''
                                    full_script += apr_script
                                else :
                                    apr_script=' '
                                if 'May' in ss2020:
                                    if full_script:
                                        full_script += ' or '
                                    may_script=f'''  {date_type} between \'{year}-05-01\' and \'{year}-06-01\' '''
                                    full_script += may_script
                                else :
                                    may_script=' '
                                if 'İyun' in ss2020:
                                    if full_script:
                                        full_script += ' or '
                                    jun_script=f'''  {date_type} between \'{year}-06-01\' and \'{year}-07-01\' '''
                                    full_script += jun_script
                                else :
                                    jun_script=' '
                                if 'İyul' in ss2020:
                                    if full_script:
                                        full_script += ' or '
                                    jul_script=f'''  {date_type} between \'{year}-07-01\' and \'{year}-08-01\' '''
                                    full_script += jul_script
                                else :
                                    jul_script=' '
                                if 'Avqust' in ss2020:
                                    if full_script:
                                        full_script += ' or '
                                    avg_script=f'''  {date_type} between \'{year}-08-01\' and \'{year}-09-01\' '''
                                    full_script += avg_script
                                else :
                                    avg_script=' '
                                if 'Sentyabr' in ss2020:
                                    if full_script:
                                        full_script += ' or '
                                    sen_script=f'''  {date_type} between \'{year}-09-01\' and \'{year}-10-01\' '''
                                    full_script += sen_script
                                else :
                                    sen_script=' '
                                if 'Oktyabr' in ss2020:
                                    if full_script:
                                        full_script += ' or '
                                    oct_script=f'''  {date_type} between \'{year}-10-01\' and \'{year}-11-01\' '''
                                    full_script += oct_script
                                else :
                                    oct_script=' '
                                if 'Noyabr' in ss2020:
                                    if full_script:
                                        full_script += ' or '
                                    nov_script=f'''  {date_type} between \'{year}-11-01\' and \'{year}-12-01\' '''
                                    full_script += nov_script
                                else :
                                    nov_script=' '
                                if 'Dekabr' in ss2020:
                                    if full_script:
                                        full_script += ' or '
                                    dec_script=f'''  {date_type} between \'{year}-12-01\' and \'{year+1}-01-01\' '''
                                    full_script += dec_script
                                else :
                                    dec_script=' '
                        if s2021:
                            if ss2021!= ' ':
                                #full_script = ''  
                                year=2021 
                                if 'Yanvar' in ss2021:
                                    if full_script:
                                        full_script += ' or '
                                    jan_script=f'''  {date_type} between \'{year}-01-01\' and \'{year}-02-01\' '''
                                    full_script += jan_script
                                else :
                                    jan_script=' '
                                if 'Fevral' in ss2021:
                                    feb_script=f'''  {date_type} between \'{year}-02-01\' and \'{year}-03-01\' '''
                                    if full_script:
                                        full_script += ' or '
                                    full_script += feb_script
                                else :
                                    feb_script=' '
                                if 'Mart' in ss2021:
                                    if full_script:
                                        full_script += ' or '
                                    mar_script=f'''  {date_type} between \'{year}-03-01\' and \'{year}-04-01\' '''
                                    full_script += mar_script
                                else :
                                    mar_script=' '
                                if 'Aprel' in ss2021:
                                    if full_script:
                                        full_script += ' or '
                                    apr_script=f'''  {date_type} between \'{year}-04-01\' and \'{year}-05-01\' '''
                                    full_script += apr_script
                                else :
                                    apr_script=' '
                                if 'May' in ss2021:
                                    if full_script:
                                        full_script += ' or '
                                    may_script=f'''  {date_type} between \'{year}-05-01\' and \'{year}-06-01\' '''
                                    full_script += may_script
                                else :
                                    may_script=' '
                                if 'İyun' in ss2021:
                                    if full_script:
                                        full_script += ' or '
                                    jun_script=f'''  {date_type} between \'{year}-06-01\' and \'{year}-07-01\' '''
                                    full_script += jun_script
                                else :
                                    jun_script=' '
                                if 'İyul' in ss2021:
                                    if full_script:
                                        full_script += ' or '
                                    jul_script=f'''  {date_type} between \'{year}-07-01\' and \'{year}-08-01\' '''
                                    full_script += jul_script
                                else :
                                    jul_script=' '
                                if 'Avqust' in ss2021:
                                    if full_script:
                                        full_script += ' or '
                                    avg_script=f'''  {date_type} between \'{year}-08-01\' and \'{year}-09-01\' '''
                                    full_script += avg_script
                                else :
                                    avg_script=' '
                                if 'Sentyabr' in ss2021:
                                    if full_script:
                                        full_script += ' or '
                                    sen_script=f'''  {date_type} between \'{year}-09-01\' and \'{year}-10-01\' '''
                                    full_script += sen_script
                                else :
                                    sen_script=' '
                                if 'Oktyabr' in ss2021:
                                    if full_script:
                                        full_script += ' or '
                                    oct_script=f'''  {date_type} between \'{year}-10-01\' and \'{year}-11-01\' '''
                                    full_script += oct_script
                                else :
                                    oct_script=' '
                                if 'Noyabr' in ss2021:
                                    if full_script:
                                        full_script += ' or '
                                    nov_script=f'''  {date_type} between \'{year}-11-01\' and \'{year}-12-01\' '''
                                    full_script += nov_script
                                else :
                                    nov_script=' '
                                if 'Dekabr' in ss2021:
                                    if full_script:
                                        full_script += ' or '
                                    dec_script=f'''  {date_type} between \'{year}-12-01\' and \'{year+1}-01-01\' '''
                                    full_script += dec_script
                                else :
                                    dec_script=' '
                        if s2022:
                            if ss2022!= ' ':
                                #full_script = ''   
                                year=2022
                                if 'Yanvar' in ss2022:
                                    if full_script:
                                        full_script += ' or '
                                    jan_script=f'''  {date_type} between \'{year}-01-01\' and \'{year}-02-01\' '''
                                    full_script += jan_script
                                else :
                                    jan_script=' '
                                if 'Fevral' in ss2022:
                                    feb_script=f'''  {date_type} between \'{year}-02-01\' and \'{year}-03-01\' '''
                                    if full_script:
                                        full_script += ' or '
                                    full_script += feb_script
                                else :
                                    feb_script=' '
                                if 'Mart' in ss2022:
                                    if full_script:
                                        full_script += ' or '
                                    mar_script=f'''  {date_type} between \'{year}-03-01\' and \'{year}-04-01\' '''
                                    full_script += mar_script
                                else :
                                    mar_script=' '
                                if 'Aprel' in ss2022:
                                    if full_script:
                                        full_script += ' or '
                                    apr_script=f'''  {date_type} between \'{year}-04-01\' and \'{year}-05-01\' '''
                                    full_script += apr_script
                                else :
                                    apr_script=' '
                                if 'May' in ss2022:
                                    if full_script:
                                        full_script += ' or '
                                    may_script=f'''  {date_type} between \'{year}-05-01\' and \'{year}-06-01\' '''
                                    full_script += may_script
                                else :
                                    may_script=' '
                                if 'İyun' in ss2022:
                                    if full_script:
                                        full_script += ' or '
                                    jun_script=f'''  {date_type} between \'{year}-06-01\' and \'{year}-07-01\' '''
                                    full_script += jun_script
                                else :
                                    jun_script=' '
                                if 'İyul' in ss2022:
                                    if full_script:
                                        full_script += ' or '
                                    jul_script=f'''  {date_type} between \'{year}-07-01\' and \'{year}-08-01\' '''
                                    full_script += jul_script
                                else :
                                    jul_script=' '
                                if 'Avqust' in ss2022:
                                    if full_script:
                                        full_script += ' or '
                                    avg_script=f'''  {date_type} between \'{year}-08-01\' and \'{year}-09-01\' '''
                                    full_script += avg_script
                                else :
                                    avg_script=' '
                                if 'Sentyabr' in ss2022:
                                    if full_script:
                                        full_script += ' or '
                                    sen_script=f'''  {date_type} between \'{year}-09-01\' and \'{year}-10-01\' '''
                                    full_script += sen_script
                                else :
                                    sen_script=' '
                                if 'Oktyabr' in ss2022:
                                    if full_script:
                                        full_script += ' or '
                                    oct_script=f'''  {date_type} between \'{year}-10-01\' and \'{year}-11-01\' '''
                                    full_script += oct_script
                                else :
                                    oct_script=' '
                                if 'Noyabr' in ss2022:
                                    if full_script:
                                        full_script += ' or '
                                    nov_script=f'''  {date_type} between \'{year}-11-01\' and \'{year}-12-01\' '''
                                    full_script += nov_script
                                else :
                                    nov_script=' '
                                if 'Dekabr' in ss2022:
                                    if full_script:
                                        full_script += ' or '
                                    dec_script=f'''  {date_type} between \'{year}-12-01\' and \'{year+1}-01-01\' '''
                                    full_script += dec_script
                                else :
                                    dec_script=' '
                        if s2023:
                            if ss2023!= ' ':
                                #full_script = ''   
                                year=2023
                                if 'Yanvar' in ss2023:
                                    if full_script:
                                        full_script += ' or '
                                    jan_script=f'''  {date_type} between \'{year}-01-01\' and \'{year}-02-01\' '''
                                    full_script += jan_script
                                else :
                                    jan_script=' '
                                if 'Fevral' in ss2023:
                                    feb_script=f'''  {date_type} between \'{year}-02-01\' and \'{year}-03-01\' '''
                                    if full_script:
                                        full_script += ' or '
                                    full_script += feb_script
                                else :
                                    feb_script=' '
                                if 'Mart' in ss2023:
                                    if full_script:
                                        full_script += ' or '
                                    mar_script=f'''  {date_type} between \'{year}-03-01\' and \'{year}-04-01\' '''
                                    full_script += mar_script
                                else :
                                    mar_script=' '
                                if 'Aprel' in ss2023:
                                    if full_script:
                                        full_script += ' or '
                                    apr_script=f'''  {date_type} between \'{year}-04-01\' and \'{year}-05-01\' '''
                                    full_script += apr_script
                                else :
                                    apr_script=' '
                                if 'May' in ss2023:
                                    if full_script:
                                        full_script += ' or '
                                    may_script=f'''  {date_type} between \'{year}-05-01\' and \'{year}-06-01\' '''
                                    full_script += may_script
                                else :
                                    may_script=' '
                                if 'İyun' in ss2023:
                                    if full_script:
                                        full_script += ' or '
                                    jun_script=f'''  {date_type} between \'{year}-06-01\' and \'{year}-07-01\' '''
                                    full_script += jun_script
                                else :
                                    jun_script=' '
                                if 'İyul' in ss2023:
                                    if full_script:
                                        full_script += ' or '
                                    jul_script=f'''  {date_type} between \'{year}-07-01\' and \'{year}-08-01\' '''
                                    full_script += jul_script
                                else :
                                    jul_script=' '
                                if 'Avqust' in ss2023:
                                    if full_script:
                                        full_script += ' or '
                                    avg_script=f'''  {date_type} between \'{year}-08-01\' and \'{year}-09-01\' '''
                                    full_script += avg_script
                                else :
                                    avg_script=' '
                                if 'Sentyabr' in ss2023:
                                    if full_script:
                                        full_script += ' or '
                                    sen_script=f'''  {date_type} between \'{year}-09-01\' and \'{year}-10-01\' '''
                                    full_script += sen_script
                                else :
                                    sen_script=' '
                                if 'Oktyabr' in ss2023:
                                    if full_script:
                                        full_script += ' or '
                                    oct_script=f'''  {date_type} between \'{year}-10-01\' and \'{year}-11-01\' '''
                                    full_script += oct_script
                                else :
                                    oct_script=' '
                                if 'Noyabr' in ss2023:
                                    if full_script:
                                        full_script += ' or '
                                    nov_script=f'''  {date_type} between \'{year}-11-01\' and \'{year}-12-01\' '''
                                    full_script += nov_script
                                else :
                                    nov_script=' '
                                if 'Dekabr' in ss2023:
                                    if full_script:
                                        full_script += ' or '
                                    dec_script=f'''  {date_type} between \'{year}-12-01\' and \'{year+1}-01-01\' '''
                                    full_script += dec_script
                                else :
                                    dec_script=' '
                        

                        
                        ss,dd=st.sidebar.columns(2)
                        last_fifteen=ss.checkbox("Son 15 gün")
                        last_thirty=dd.checkbox("Son 30 gün")
                        all_column=('id_operation', "Provider", "Service", "Agent", 'provider_trans', 'id_point', 'account', 'account2', 
                                        'status','time_server', 'time_process', 'sum_income', 'sum_outcome', 'cashcount', "1M", "5M", "10M", 
                                        "20M", "50M", "100M", "200M", 'id_money_collection', 'collection_time', "comment", 'TotalCashCount' ,
                                        'TotalAmount')
                        defaultt=('id_operation', "Provider", "Service", "Agent", 'id_point', 'account', 'account2', 
                                        'status','time_server', 'sum_income', 'sum_outcome', 'cashcount', "1M", "5M", "10M", 
                                        "20M", "50M", "100M", "200M")
                            
                        column_choice=st.sidebar.multiselect(" ",all_column,default=defaultt)

                        column_choice=tuple(column_choice)
                        column_choice='","'.join(column_choice)
                        column_choice='"'+column_choice+'"'
                        
                        cf=st.sidebar.button("Filterləri təmizlə")
                            
                        if  last_fifteen:
                            if full_script:
                                full_script += ' or '
                            start_date,end_date=funcc.getlast15date()
                            lst15=f'''{date_type} between  \'{start_date}\' and \'{end_date}\''''
                            full_script += lst15
                        else :
                            last_fifteen=False
                            lst15=False
                                    #st.write(start_date,end_date)
                        if  last_thirty:
                            if full_script:
                                full_script += ' or '
                            start_date,end_date=funcc.getlast30date()
                            lst30=f'''{date_type} between  \'{start_date}\' and \'{end_date}\''''
                            full_script += lst30
                        else: 
                            last_thirty=False
                            lst30=False
                        
                        if  cf == False :
                            cf=False
                            clrfltr=' '
                        else :
                            start_date,end_date=funcc.getdate()
                            #st.write(start_date,end_date)
                            full_script=f'''  {date_type} between  \'{start_date}\' and \'{end_date}\' ''' 
                            lst30=True  
                            lst15=True
                        if full_script:
                            scripts = f" ({full_script.strip()})"
                        else :
                            start_date,end_date=funcc.getdate()
                            #st.write(start_date,end_date)
                            scripts=f'''  {date_type} between  \'{start_date}\' and \'{end_date}\' '''
                        
                        if date_typee=="Ödəniş tarixi":
                            sdas=scripts.replace('time_server','createtime')
                        else :
                            sdas=scripts.replace('g.collection_time','cashoutdate ')
                                
                       
                        b=('Provider', 'Service','Agent','id_operation','id_point', 'account','account2','status','sum_income','sum_outcome','time_server_date',
                           'time_process_date','cashcount','provider_trans','1M','5M','10M','20M','50M','100M','200M','id_money_collection','time_server','time_process','collection_time','comment')
                        selected=st.sidebar.multiselect("Sütun",b)
                        add=selected
                           
                        selected=tuple(selected) 
                        if selected==():
                            selected=list(b)
                          
                        if "time_server_date" in selected:
                            selected.remove("time_server_date")
                        if "time_process_date" in selected:
                            selected.remove("time_process_date")
                        
                        if len(selected)>1:
                            selected = selected
                        else:
                            selected = tuple(selected)
                            selected = "('{}')".format(selected[0]) 
                        
                        a4=st.sidebar.checkbox("Yarat")
                        if a4:
                            vv='Report '
                            st.write(scripts)
                            st.header (vv)
                                    
                           
                            
                                     

                            SQl_query_filtered=mpay_payment_checks(conn,column_choice,scripts,mp_pr,mp_sr,mpay_point,status,mpay_ccount)
                            
                            #SQl_query_filtered = SQl_query_filtered.fillna('')
                            SQl_query_filtered = SQl_query_filtered.fillna('')
                            #funcc.dataframee(SQl_query_filtered)
                            a=pd.DataFrame(SQl_query_filtered)
                            if 'time_server' in a:
                                a["time_server_date"]= pd.to_datetime(a['time_server']).dt.date
                                a["time_server_date"] = a["time_server_date"].apply(lambda x: x.strftime("%Y-%m-%d"))
                            if 'time_process' in a:
                                a["time_process_date"]= pd.to_datetime(a['time_process']).dt.date
                                a["time_process_date"] = a["time_process_date"].apply(lambda x: x.strftime("%Y-%m-%d"))
                            #st.write(a)
                                #st.write(SQl_query_filtered)
                            
                                
                                
                                       
                            
                            exc=st.sidebar.checkbox("Download all data")
                            if exc:
                                excel=funcc.to_excell(SQl_query_filtered)
                                
                                st.sidebar.download_button(label="EXCEL",data=excel,file_name=vv+'.xlsx',mime='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
                                csv = funcc.convert_df(SQl_query_filtered)
                                    
                                st.sidebar.download_button(label="CSV",data=csv,file_name=vv+'.csv',mime='text/csv'    )
                                st.sidebar.download_button(label="TEXT",data=csv,file_name=vv+'.txt',mime='text')
                            exxc=st.sidebar.checkbox("Download data")
                            if exxc:
                                excel=funcc.to_excell(a)
                                
                                st.sidebar.download_button(label="EXCEL",data=excel,file_name=vv+'.xlsx',mime='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
                                csv = funcc.convert_df(a)
                                    
                                st.sidebar.download_button(label="CSV",data=csv,file_name=vv+'.csv',mime='text/csv'    )
                                st.sidebar.download_button(label="TEXT",data=csv,file_name=vv+'.txt',mime='text')
                            
                            b=a.columns 
                            
                            
                            funcc.dataframmee111(a.head(10))     
                            numeric_cols=['sum_income','sum_outcome']
                                        
                            result = pd.DataFrame([{col: int(a[col].sum()) for col in numeric_cols}]).T
                                        
                            result = result.reset_index(drop=True).T
                            result.columns=['sum_income','sum_outcome']
                            result = result.reset_index()
                                        
                            result.columns=[' ','sum(sum_income)','sum(sum_outcome)']
                            result["count"]=pd.DataFrame(a.count()).iloc[0,0]
                            result.iloc[0,0]='Total'
                            #result = result.set_index('Total')
                            funcc.dataframee1(result)          
                            bas=st.checkbox('Pivot')                      
                            if bas:   
                                
                                selected=list(selected)                
                                df=a[selected]
                                #st.write(df)
                                funcc.dataframmee(df)
                            pag=st.checkbox('pagination')
                            if pag:    
                                numeric_cols=['sum_income','sum_outcome','cashcount','1M','5M','10M','20M','50M','100M','200M']
                                selected=list(selected)  
                                #st.write(selected)
                                df=a[selected]
                                string_col=b[~b.isin(numeric_cols)].tolist()
                                lenn=len(df)
                                page_size = st.slider('Number of rows per page', min_value=10, max_value=lenn, step=10, value=20)
                                page_number = st.number_input('Page number', min_value=1, value=1)
                                pivot_columns = st.multiselect('Pivot columns', numeric_cols)
                                groupby_columns = st.multiselect('Groupby columns', string_col)
                                

                                #df = df.fillna('No Value')

                                cr=st.checkbox('Create')
                                #if pivot_columns != [] and  groupby_columns != []:
                                if cr:
                                # применение пагинации и пивот-фильтров к данным
                                    
                                    data = pd.pivot_table(df, index=groupby_columns,  values=pivot_columns, aggfunc=np.sum)
                                    #data=pd.pivot_table(df,index='account',values='cashcount',aggfunc=np.sum)
                                    dataa = data.iloc[(page_number-1)*page_size:page_number*page_size]

                                    # отображение данных в таблице
                                    
                                    funcc.dataframee(dataa.reset_index())
                                else :
                                    st.warning('Sütunları əlavə edin')
                                exxc=st.sidebar.checkbox("Download paginated data")
                                if exxc:
                                    data=data.reset_index()
                                    excel=funcc.to_excell(data)
                                    
                                    st.sidebar.download_button(label="EXCEL",data=excel,file_name=vv+'.xlsx',mime='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
                                    csv = funcc.convert_df(data)
                                        
                                    st.sidebar.download_button(label="CSV",data=csv,file_name=vv+'.csv',mime='text/csv'    )
                                    st.sidebar.download_button(label="TEXT",data=csv,file_name=vv+'.txt',mime='text')
                                        
                            
                               
                            
                        else:
                            st.markdown(
    """
    <style>
        div [data-testid=stImage]{
            text-align: center;
            display: block;
            margin-left: auto;
            margin-right: auto;
            width: 100%;
            margin-top: 20%;
        }

    </style>
    """, unsafe_allow_html=True
)
                            icon=Image.open('edatamart.png')        
                            st.image(icon)    
                                
                elif  company=="Modenis" :
                    servicenamemod=servicename[servicename['Service ID'].notnull()]      
                         
                    if company=='Modenis' :
                        if allmodeniss_container:
                            prrnamemod=np.unique(servicenamemod["Provider yeni adlar"])
                            
                            prrasmod = st.sidebar.multiselect('Provider', prrnamemod,help='Bütün providerləri seçmək üçün heç bir seçim etməyin')
                            if prrasmod !=[]:
                                prname=servicename[servicename["Provider yeni adlar"].isin(prrasmod)]
                               # st.write(prname)
                                if len(prrasmod)==1:
                                    prrasmod     = tuple(prrasmod)
                                    prrasmod     = "('{}')".format(prrasmod    [0]) 
                                    mp_pr=f' and "Provider" in {prrasmod}'
                                    
                                elif len(prrasmod)>1 :
                                    mp_pr=f' and  "Provider" in {tuple(prrasmod)}'
                                
                            else :
                                prname=servicename
                                mp_pr=' '
                            sr_name=np.unique(prname["Service name yeni adlar"])
                            servmod = st.sidebar.multiselect('Xidmət',sr_name)
                            if servmod==[]:
                                servmod=sr_name
                                mp_sr=' '
                            elif len(servmod)==1:
                                servmod     = tuple(servmod)
                                servmod     = "('{}')".format(servmod    [0]) 
                                mp_sr=f' and "Service" in {servmod} '
                            else:
                                mp_sr=f' and "Service" in {tuple(servmod)} '
                            # st.write(mp_pr)
                            # st.write(mp_sr)
                                        
                            stt=('Success', 'Revoked','Processing' )
                            status=st.sidebar.multiselect("Status",stt,default="Success",help='Bütün statusları seçmək üçün heç bir seçim etməyin')
                            status=tuple(status)
                            if status ==():
                                status=stt
                                status=' '
                                    
                            elif len(status)>1:
                                status = status
                                status=f' and status in {status} '
                            elif len(status)==1:
                                status = tuple(status)
                                status = "('{}')".format(status[0])
                                status=f' and status in {status} '
                            
                            mod_chashout=st.sidebar.text_input('İnkassasiya id')
                            if mod_chashout!='':
                                mod_ccount=f' and m.cashoutid in ({mod_chashout})'
                            else:
                                mod_ccount=' '
                            new_row=pd.Series(['All'])
                                #pointidd_mod = point_mod.append(new_row, ignore_index=True)
                            pointidd_mod=pd.concat([point_mod,new_row])
                            pointmod=st.sidebar.multiselect("Pointid",pointidd_mod,default='All')
                            pointmod=tuple(pointmod) 
                            if len(pointmod)>1:
                                pointmod = pointmod
                            else:
                                pointmod = tuple(pointmod)
                                pointmod = "('{}')".format(pointmod[0])               
                            if 'All' in pointmod:
                                point_mod= ' '
                                
                            else : 
                                point_mod= f' pointid in {pointmod} and '
                           
                            # if mod_provider_slb ==[]:
                            #     mod_pr=' '
                            # else :
                            #     mod_pr=f'and nomenclaturename in {mod_provider_slb} '
                            
                            st.sidebar.header("Tarixi seçin:")
                            date_typee=st.sidebar.radio("",("Ödəniş tarixi","İnkassasiya tarixi"),horizontal=True)
                            if date_typee=="Ödəniş tarixi":
                                date_type=' createtime '
                            else:
                                date_type=' cashoutdate '
                            a,b=st.sidebar.columns(2)     
                                
                            v=datetime.today().replace(day=1)
                            div=st.sidebar.container()
                            full_script=''
                            start_date=a.date_input("Başlanğıc tarix",v)
                            end_date=b.date_input("Son tarix")
                            f=a.checkbox("Filter")
                            st.sidebar.write(' ')
                            st.sidebar.write(' ')
                            c,d=st.sidebar.columns(2)
                            s2020=c.checkbox("2020")
                            s2021=d.checkbox("2021")
                            s2022=c.checkbox("2022")
                            s2023=d.checkbox("2023")
                            a=('Yanvar','Fevral','Mart','Aprel','May','İyun','İyul','Avqust','Sentyabr','Oktyabr','Noyabr','Dekabr')
                            full_script = '' 
                            if f:
                                start_date=start_date
                                end_date=end_date
                                ccc=f'''  {date_type} between '{start_date}' and '{end_date}' '''
                                if full_script:
                                    full_script += ' or '
                                full_script += ccc
                            if s2020:
                                year=2020
                                ss2020=st.sidebar.multiselect('2020',a)
                            if s2021:
                                year=2021
                                ss2021=st.sidebar.multiselect('2021 ',a)
                            if s2022:
                                year=2022
                                ss2022=st.sidebar.multiselect('2022 ',a)
                            if s2023:
                                year=2023
                                ss2023=st.sidebar.multiselect('2023 ',a)
                            if s2020:
                                if ss2020!= ' ':
                                    year=2020
                                    #full_script = ''   
                                    if 'Yanvar' in ss2020:
                                        jan_script=f'''  {date_type} between \'{year}-01-01\' and \'{year}-02-01\' '''
                                        if full_script:
                                            full_script += ' or '
                                        full_script += jan_script
                                    else :
                                        jan_script=' '
                                    if 'Fevral' in ss2020:
                                        feb_script=f'''  {date_type} between \'{year}-02-01\' and \'{year}-03-01\' '''
                                        if full_script:
                                            full_script += ' or '
                                        full_script += feb_script
                                    else :
                                        feb_script=' '
                                    if 'Mart' in ss2020:
                                        if full_script:
                                            full_script += ' or '
                                        mar_script=f'''  {date_type} between \'{year}-03-01\' and \'{year}-04-01\' '''
                                        full_script += mar_script
                                    else :
                                        mar_script=' '
                                    if 'Aprel' in ss2020:
                                        if full_script:
                                            full_script += ' or '
                                        apr_script=f'''  {date_type} between \'{year}-04-01\' and \'{year}-05-01\' '''
                                        full_script += apr_script
                                    else :
                                        apr_script=' '
                                    if 'May' in ss2020:
                                        if full_script:
                                            full_script += ' or '
                                        may_script=f'''  {date_type} between \'{year}-05-01\' and \'{year}-06-01\' '''
                                        full_script += may_script
                                    else :
                                        may_script=' '
                                    if 'İyun' in ss2020:
                                        if full_script:
                                            full_script += ' or '
                                        jun_script=f'''  {date_type} between \'{year}-06-01\' and \'{year}-07-01\' '''
                                        full_script += jun_script
                                    else :
                                        jun_script=' '
                                    if 'İyul' in ss2020:
                                        if full_script:
                                            full_script += ' or '
                                        jul_script=f'''  {date_type} between \'{year}-07-01\' and \'{year}-08-01\' '''
                                        full_script += jul_script
                                    else :
                                        jul_script=' '
                                    if 'Avqust' in ss2020:
                                        if full_script:
                                            full_script += ' or '
                                        avg_script=f'''  {date_type} between \'{year}-08-01\' and \'{year}-09-01\' '''
                                        full_script += avg_script
                                    else :
                                        avg_script=' '
                                    if 'Sentyabr' in ss2020:
                                        if full_script:
                                            full_script += ' or '
                                        sen_script=f'''  {date_type} between \'{year}-09-01\' and \'{year}-10-01\' '''
                                        full_script += sen_script
                                    else :
                                        sen_script=' '
                                    if 'Oktyabr' in ss2020:
                                        if full_script:
                                            full_script += ' or '
                                        oct_script=f'''  {date_type} between \'{year}-10-01\' and \'{year}-11-01\' '''
                                        full_script += oct_script
                                    else :
                                        oct_script=' '
                                    if 'Noyabr' in ss2020:
                                        if full_script:
                                            full_script += ' or '
                                        nov_script=f'''  {date_type} between \'{year}-11-01\' and \'{year}-12-01\' '''
                                        full_script += nov_script
                                    else :
                                        nov_script=' '
                                    if 'Dekabr' in ss2020:
                                        if full_script:
                                            full_script += ' or '
                                        dec_script=f'''  {date_type} between \'{year}-12-01\' and \'{year+1}-01-01\' '''
                                        full_script += dec_script
                                    else :
                                        dec_script=' '
                            if s2021:
                                if ss2021!= ' ':
                                    #full_script = ''  
                                    year=2021 
                                    if 'Yanvar' in ss2021:
                                        if full_script:
                                            full_script += ' or '
                                        jan_script=f'''  {date_type} between \'{year}-01-01\' and \'{year}-02-01\' '''
                                        full_script += jan_script
                                    else :
                                        jan_script=' '
                                    if 'Fevral' in ss2021:
                                        feb_script=f'''  {date_type} between \'{year}-02-01\' and \'{year}-03-01\' '''
                                        if full_script:
                                            full_script += ' or '
                                        full_script += feb_script
                                    else :
                                        feb_script=' '
                                    if 'Mart' in ss2021:
                                        if full_script:
                                            full_script += ' or '
                                        mar_script=f'''  {date_type} between \'{year}-03-01\' and \'{year}-04-01\' '''
                                        full_script += mar_script
                                    else :
                                        mar_script=' '
                                    if 'Aprel' in ss2021:
                                        if full_script:
                                            full_script += ' or '
                                        apr_script=f'''  {date_type} between \'{year}-04-01\' and \'{year}-05-01\' '''
                                        full_script += apr_script
                                    else :
                                        apr_script=' '
                                    if 'May' in ss2021:
                                        if full_script:
                                            full_script += ' or '
                                        may_script=f'''  {date_type} between \'{year}-05-01\' and \'{year}-06-01\' '''
                                        full_script += may_script
                                    else :
                                        may_script=' '
                                    if 'İyun' in ss2021:
                                        if full_script:
                                            full_script += ' or '
                                        jun_script=f'''  {date_type} between \'{year}-06-01\' and \'{year}-07-01\' '''
                                        full_script += jun_script
                                    else :
                                        jun_script=' '
                                    if 'İyul' in ss2021:
                                        if full_script:
                                            full_script += ' or '
                                        jul_script=f'''  {date_type} between \'{year}-07-01\' and \'{year}-08-01\' '''
                                        full_script += jul_script
                                    else :
                                        jul_script=' '
                                    if 'Avqust' in ss2021:
                                        if full_script:
                                            full_script += ' or '
                                        avg_script=f'''  {date_type} between \'{year}-08-01\' and \'{year}-09-01\' '''
                                        full_script += avg_script
                                    else :
                                        avg_script=' '
                                    if 'Sentyabr' in ss2021:
                                        if full_script:
                                            full_script += ' or '
                                        sen_script=f'''  {date_type} between \'{year}-09-01\' and \'{year}-10-01\' '''
                                        full_script += sen_script
                                    else :
                                        sen_script=' '
                                    if 'Oktyabr' in ss2021:
                                        if full_script:
                                            full_script += ' or '
                                        oct_script=f'''  {date_type} between \'{year}-10-01\' and \'{year}-11-01\' '''
                                        full_script += oct_script
                                    else :
                                        oct_script=' '
                                    if 'Noyabr' in ss2021:
                                        if full_script:
                                            full_script += ' or '
                                        nov_script=f'''  {date_type} between \'{year}-11-01\' and \'{year}-12-01\' '''
                                        full_script += nov_script
                                    else :
                                        nov_script=' '
                                    if 'Dekabr' in ss2021:
                                        if full_script:
                                            full_script += ' or '
                                        dec_script=f'''  {date_type} between \'{year}-12-01\' and \'{year+1}-01-01\' '''
                                        full_script += dec_script
                                    else :
                                        dec_script=' '
                            if s2022:
                                if ss2022!= ' ':
                                    #full_script = ''   
                                    year=2022
                                    if 'Yanvar' in ss2022:
                                        if full_script:
                                            full_script += ' or '
                                        jan_script=f'''  {date_type} between \'{year}-01-01\' and \'{year}-02-01\' '''
                                        full_script += jan_script
                                    else :
                                        jan_script=' '
                                    if 'Fevral' in ss2022:
                                        feb_script=f'''  {date_type} between \'{year}-02-01\' and \'{year}-03-01\' '''
                                        if full_script:
                                            full_script += ' or '
                                        full_script += feb_script
                                    else :
                                        feb_script=' '
                                    if 'Mart' in ss2022:
                                        if full_script:
                                            full_script += ' or '
                                        mar_script=f'''  {date_type} between \'{year}-03-01\' and \'{year}-04-01\' '''
                                        full_script += mar_script
                                    else :
                                        mar_script=' '
                                    if 'Aprel' in ss2022:
                                        if full_script:
                                            full_script += ' or '
                                        apr_script=f'''  {date_type} between \'{year}-04-01\' and \'{year}-05-01\' '''
                                        full_script += apr_script
                                    else :
                                        apr_script=' '
                                    if 'May' in ss2022:
                                        if full_script:
                                            full_script += ' or '
                                        may_script=f'''  {date_type} between \'{year}-05-01\' and \'{year}-06-01\' '''
                                        full_script += may_script
                                    else :
                                        may_script=' '
                                    if 'İyun' in ss2022:
                                        if full_script:
                                            full_script += ' or '
                                        jun_script=f'''  {date_type} between \'{year}-06-01\' and \'{year}-07-01\' '''
                                        full_script += jun_script
                                    else :
                                        jun_script=' '
                                    if 'İyul' in ss2022:
                                        if full_script:
                                            full_script += ' or '
                                        jul_script=f'''  {date_type} between \'{year}-07-01\' and \'{year}-08-01\' '''
                                        full_script += jul_script
                                    else :
                                        jul_script=' '
                                    if 'Avqust' in ss2022:
                                        if full_script:
                                            full_script += ' or '
                                        avg_script=f'''  {date_type} between \'{year}-08-01\' and \'{year}-09-01\' '''
                                        full_script += avg_script
                                    else :
                                        avg_script=' '
                                    if 'Sentyabr' in ss2022:
                                        if full_script:
                                            full_script += ' or '
                                        sen_script=f'''  {date_type} between \'{year}-09-01\' and \'{year}-10-01\' '''
                                        full_script += sen_script
                                    else :
                                        sen_script=' '
                                    if 'Oktyabr' in ss2022:
                                        if full_script:
                                            full_script += ' or '
                                        oct_script=f'''  {date_type} between \'{year}-10-01\' and \'{year}-11-01\' '''
                                        full_script += oct_script
                                    else :
                                        oct_script=' '
                                    if 'Noyabr' in ss2022:
                                        if full_script:
                                            full_script += ' or '
                                        nov_script=f'''  {date_type} between \'{year}-11-01\' and \'{year}-12-01\' '''
                                        full_script += nov_script
                                    else :
                                        nov_script=' '
                                    if 'Dekabr' in ss2022:
                                        if full_script:
                                            full_script += ' or '
                                        dec_script=f'''  {date_type} between \'{year}-12-01\' and \'{year+1}-01-01\' '''
                                        full_script += dec_script
                                    else :
                                        dec_script=' '
                            if s2023:
                                if ss2023!= ' ':
                                    #full_script = ''   
                                    year=2023
                                    if 'Yanvar' in ss2023:
                                        if full_script:
                                            full_script += ' or '
                                        jan_script=f'''  {date_type} between \'{year}-01-01\' and \'{year}-02-01\' '''
                                        full_script += jan_script
                                    else :
                                        jan_script=' '
                                    if 'Fevral' in ss2023:
                                        feb_script=f'''  {date_type} between \'{year}-02-01\' and \'{year}-03-01\' '''
                                        if full_script:
                                            full_script += ' or '
                                        full_script += feb_script
                                    else :
                                        feb_script=' '
                                    if 'Mart' in ss2023:
                                        if full_script:
                                            full_script += ' or '
                                        mar_script=f'''  {date_type} between \'{year}-03-01\' and \'{year}-04-01\' '''
                                        full_script += mar_script
                                    else :
                                        mar_script=' '
                                    if 'Aprel' in ss2023:
                                        if full_script:
                                            full_script += ' or '
                                        apr_script=f'''  {date_type} between \'{year}-04-01\' and \'{year}-05-01\' '''
                                        full_script += apr_script
                                    else :
                                        apr_script=' '
                                    if 'May' in ss2023:
                                        if full_script:
                                            full_script += ' or '
                                        may_script=f'''  {date_type} between \'{year}-05-01\' and \'{year}-06-01\' '''
                                        full_script += may_script
                                    else :
                                        may_script=' '
                                    if 'İyun' in ss2023:
                                        if full_script:
                                            full_script += ' or '
                                        jun_script=f'''  {date_type} between \'{year}-06-01\' and \'{year}-07-01\' '''
                                        full_script += jun_script
                                    else :
                                        jun_script=' '
                                    if 'İyul' in ss2023:
                                        if full_script:
                                            full_script += ' or '
                                        jul_script=f'''  {date_type} between \'{year}-07-01\' and \'{year}-08-01\' '''
                                        full_script += jul_script
                                    else :
                                        jul_script=' '
                                    if 'Avqust' in ss2023:
                                        if full_script:
                                            full_script += ' or '
                                        avg_script=f'''  {date_type} between \'{year}-08-01\' and \'{year}-09-01\' '''
                                        full_script += avg_script
                                    else :
                                        avg_script=' '
                                    if 'Sentyabr' in ss2023:
                                        if full_script:
                                            full_script += ' or '
                                        sen_script=f'''  {date_type} between \'{year}-09-01\' and \'{year}-10-01\' '''
                                        full_script += sen_script
                                    else :
                                        sen_script=' '
                                    if 'Oktyabr' in ss2023:
                                        if full_script:
                                            full_script += ' or '
                                        oct_script=f'''  {date_type} between \'{year}-10-01\' and \'{year}-11-01\' '''
                                        full_script += oct_script
                                    else :
                                        oct_script=' '
                                    if 'Noyabr' in ss2023:
                                        if full_script:
                                            full_script += ' or '
                                        nov_script=f'''  {date_type} between \'{year}-11-01\' and \'{year}-12-01\' '''
                                        full_script += nov_script
                                    else :
                                        nov_script=' '
                                    if 'Dekabr' in ss2023:
                                        if full_script:
                                            full_script += ' or '
                                        dec_script=f'''  {date_type} between \'{year}-12-01\' and \'{year+1}-01-01\' '''
                                        full_script += dec_script
                                    else :
                                        dec_script=' '
                            

                            
                            ss,dd=st.sidebar.columns(2)
                            last_fifteen=ss.checkbox("Son 15 gün")
                            last_thirty=dd.checkbox("Son 30 gün")
                            cf=st.sidebar.button("Filterləri təmizlə")
                                
                            if  last_fifteen:
                                if full_script:
                                    full_script += ' or '
                                start_date,end_date=funcc.getlast15date()
                                lst15=f'''{date_type} between  \'{start_date}\' and \'{end_date}\''''
                                full_script += lst15
                            else :
                                last_fifteen=False
                                lst15=False
                                        #st.write(start_date,end_date)
                            if  last_thirty:
                                if full_script:
                                    full_script += ' or '
                                start_date,end_date=funcc.getlast30date()
                                lst30=f'''{date_type} between  \'{start_date}\' and \'{end_date}\''''
                                full_script += lst30
                            else: 
                                last_thirty=False
                                lst30=False
                            
                            if  cf == False :
                                cf=False
                                clrfltr=' '
                            else :
                                start_date,end_date=funcc.getdate()
                                #st.write(start_date,end_date)
                                full_script=f'''  {date_type} between  \'{start_date}\' and \'{end_date}\' ''' 
                                lst30=True  
                                lst15=True
                            if full_script:
                                scripts = f" ({full_script.strip()})"
                            else :
                                start_date,end_date=funcc.getdate()
                                #st.write(start_date,end_date)
                                scripts=f'''  {date_type} between  \'{start_date}\' and \'{end_date}\' '''
                            
                            b=('pointid','Number','status','payvalue', 'servicevalue','providerprofit','providerprofitvalue','status_date','create_date')
                            selected=st.sidebar.multiselect("Sütun",b)
                            
                            if selected==[]:
                                selected=list(b)
                                #st.write(servi_mod)
                            b2=st.sidebar.checkbox("Yarat")
                            if b2 is True:
                                
                                    
                                vv='Report '
                                st.header (vv)
                                    
                                a=modenis_paymentcheck(conn,mp_sr,point_mod,scripts,status,mp_pr,mod_ccount)
                                exc=st.sidebar.checkbox("Download all data")
                                if exc:
                                    excel=funcc.to_excell(a)
                                    st.sidebar.download_button(label="EXCEL",data=excel,file_name=vv+'.xlsx',mime='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
                                    csv = funcc.convert_df(a)
                                
                                    st.sidebar.download_button(label="CSV",data=csv,file_name=vv+'.csv',mime='text/csv'    )
                                    st.sidebar.download_button(label="TEXT",data=csv,file_name=vv+'.txt',mime='text')
                                a=pd.DataFrame(a)
                                a["create_date"]= pd.to_datetime(a['createtime']).dt.date
                                a["create_date"] = a["create_date"].apply(lambda x: x.strftime("%Y-%m-%d"))
                                a["status_date"]= pd.to_datetime(a['statustime']).dt.date
                                a["status_date"] = a["status_date"].apply(lambda x: x.strftime("%Y-%m-%d"))
                                b=a.columns 
                                
                                # agg_types=st.multiselect("Agg type",['sum', 'avg', 'count'])
                                # a=a[["payvalue","servicevalue","providerprofit","providerprofitvalue","Name","statuss"]]
                                    # if 'sum' in agg_types:
                                    #     st.write('f')
                                    #     ass=a.groupby(by=selected_string).sum()
                                        
                                    #     ass = ass.rename(columns={col: 'sum(' + col+ ')' for col in ass.columns})
                                    #     ass=ass.reset_index()
                                    #     st.write(ass)
                                    # if 'avg' in agg_types:
                                    #     asss=a.groupby(by=selected_string).mean()
                                    #     asss = asss.rename(columns={col: 'avg(' + col+ ')' for col in asss.columns})
                                        
                                    #     asss=asss.reset_index()
                                    #     st.write(asss)
                                    # if 'count' in agg_types:
                                    #     aaas=a.groupby(by=selected_string).count()
                                    #     aaas = aaas.rename(columns={col: 'count(' + col+ ')' for col in aaas.columns})
                                    #     #st.write(aaas)
                                    #     aaas=aaas.reset_index()
                                    #     st.write(aaas)
                                    

                                    # final = pd.merge(ass, asss, on='Name')   
                                    # st.write(final)
                                        
                                    
                                funcc.dataframee(a.head(10))     
                                numeric_cols=['payvalue','servicevalue']
                                    
                                result = pd.DataFrame([{col: int(a[col].sum()) for col in numeric_cols}]).T
                                        
                                result = result.reset_index(drop=True).T
                                result.columns=['payvalue','servicevalue']
                                result = result.reset_index()
                                        
                                result.columns=[' ','sum(payvalue)','sum(servicevalue)']
                                result["count"]=pd.DataFrame(a.count()).iloc[0,0]
                                result.iloc[0,0]='Total'
                                #result = result.set_index('Total')
                                    
                                    
                                funcc.dataframee1(result)
                                    
                                df=a[selected]                   
                                b=b.to_series()
                                        #st.write(b)
                                        
                                numeric_cols = ["payvalue","servicevalue","providerprofit","providerprofitvalue"]
                                numeric_cols=pd.Series(numeric_cols)
                                string_col=b[~b.isin(numeric_cols)].tolist()
                                                
                                selected_numeric=pd.Series(selected)[(pd.Series(selected)).isin(numeric_cols)]
                                    #st.write(selected_numeric)
                                selected_string=pd.Series(selected)[(pd.Series(selected)).isin(string_col)].to_list()
                                
                                        
                                agg_types=st.multiselect("Agg type",['sum', 'avg', 'count'])
                                if len(agg_types)==1:   
                                        
                                                
                                    if 'sum' in agg_types:
                                        agg=sum
                                        ab=pd.DataFrame(df.groupby(by=selected_string).sum())
                                        ab=ab.reset_index()
                                    elif  'avg' in agg_types:
                                        agg=lambda x: np.mean(x).round(2)
                                        ab=pd.DataFrame(df.groupby(by=selected_string).mean().round(2))
                                        ab=ab.reset_index()
                                    else :
                                        agg=len
                                        ab=pd.DataFrame(df.groupby(by=selected_string).count())
                                        ab=ab.reset_index()
                                elif len(agg_types)==2 :
                                    if 'sum' in agg_types:
                                        ass=df.groupby(by=selected_string).sum()
                                        ass = ass.rename(columns={col: 'sum(' + col+ ')' for col in ass.columns})
                                        ass=ass.reset_index()
                                            
                                    if 'avg' in agg_types:
                                        asss=df.groupby(by=selected_string).mean().round(decimals=2)
                                        asss = asss.rename(columns={col: 'avg(' + col+ ')' for col in asss.columns})
                                        asss=asss.reset_index()
                                            
                                    if 'count' in agg_types:
                                        aaas=df.groupby(by=selected_string).count()
                                        aaas = aaas.rename(columns={col: 'count(' + col+ ')' for col in aaas.columns})
                                        #st.write(aaas)
                                        aaas=aaas.reset_index()
                                            
                                    if 'sum' in agg_types and'avg' in agg_types:
                                        final = pd.merge(ass, asss, on=selected_string)   
                                    elif 'count' in agg_types and'avg' in agg_types:
                                        final = pd.merge(aaas, asss, on=selected_string)     
                                    else:
                                        final=pd.merge(ass,aaas,on=selected_string)     
                                    funcc.dataframee(final)
                                elif len(agg_types)==3:
                                    
                                    ass=df.groupby(by=selected_string).sum()
                                    ass = ass.rename(columns={col: 'sum(' + col+ ')' for col in ass.columns})
                                    ass=ass.reset_index()
                                    
                                        
                                    asss=df.groupby(by=selected_string).mean().round(decimals=2)
                                    asss = asss.rename(columns={col: 'avg(' + col+ ')' for col in asss.columns})
                                    asss=asss.reset_index()
                                        
                                        
                                    aaas=df.groupby(by=selected_string).count()
                                    aaas = aaas.rename(columns={col: 'count(' + col+ ')' for col in aaas.columns})
                                    #st.write(aaas)
                                    aaas=aaas.reset_index()
                                            
                                            
                                    final = pd.merge(ass,aaas,  on=selected_string)
                                    final=pd.merge(final,asss,on=selected_string)   
                                    funcc.dataframee(final)
                            else :
                                st.markdown(
    """
    <style>
        div [data-testid=stImage]{
            text-align: center;
            display: block;
            margin-left: auto;
            margin-right: auto;
            width: 100%;
            margin-top: 20%;
        }

    </style>
    """, unsafe_allow_html=True
)
                                icon=Image.open('edatamart.png')        
                                st.image(icon)
                else :
                    if company=='All':
                        pr,sr,pointid_mp,pr_mod,ser_mod,point_mod,servicename=getdata(conn) 
                        sdd,point=dfg(conn)
                
                        prrname=np.unique(sdd["Provider yeni adlar"])
                    
                        #prrname=prrname.sort()
                        #st.write(prrname)
                        prras = st.sidebar.multiselect('Provider',prrname)
                        if prras !=[]:
                            prname=sdd[sdd["Provider yeni adlar"].isin(prras)]
                            #st.write(prname)
                            if len(prras)==1:
                                prras     = tuple(prras)
                                prras     = "('{}')".format(prras    [0]) 
                                mp_pr=f''' and  replace ( replace ( "Provider yeni adlar",'[MP] ','' ),'[MOD] ','' ) in {prras}'''
                                
                            elif len(prras)>1 :
                                mp_pr=f''' and  replace ( replace ( "Provider yeni adlar",'[MP] ','' ),'[MOD] ','' )in {tuple(prras)}'''
                            
                        else :
                            prname=servicename
                            mp_pr=' '
                        sr_name=np.unique(prname["Service name yeni adlar"])
                        serv = st.sidebar.multiselect('Xidmət',sr_name)
                        if serv==[]:
                            serv=sr_name
                            mp_sr=' '
                        elif len(serv)==1:
                            serv     = tuple(serv)
                            serv     = "('{}')".format(serv    [0]) 
                            mp_sr=f' and "Service" in {serv} '
                        else:
                            mp_sr=f' and "Service" in {tuple(serv)} '
                        #point=np.unique(point["pointid"])
                        
                        #st.write(point)
                        pointmpay=st.sidebar.multiselect("Pointid",point)    
                        
                        if pointmpay==[]:
                            pointmpay=pointmpay
                            pointmpay=' '
                        elif len(pointmpay)==1:
                            pointmpay     = tuple(pointmpay)
                            pointmpay     = "('{}')".format(pointmpay    [0]) 
                            pointmpay=f' and id_point in {pointmpay} '
                        else:
                            pointmpay=f' and id_point in {tuple(pointmpay)} '
                        new_row=pd.Series(['All'])
                        prrname=np.unique(point["pointid"])
                        #pointidd_mpay = pointid_mpay.append(new_row, ignore_index=True)
                        stt=('Success', 'Revoked','Processing', 'Success/Cancelled ','Success/Funds return ', 'Success/Corrected ', 'Success/Blocked ', 'Error/Cancelled by user', 'Error/Cancelled by support', 'Error/Rejected by provider', 'Error/Corrected', 'Error/Uncorrectable error', 'Error/Revocated', 'Error/Blocked by user', 'Processing/Provider determined', 'Processing/Processing', 'Processing/Unknown', 'Processing/Picked up', 'Processing/Error', 'Processing/In wait','Processing/Confirm' )
                        status=st.sidebar.multiselect("Status",stt,default="Success",help='Bütün statusları seçmək üçün heç bir seçim etməyin')
                        status=tuple(status)
                        if status ==():
                            status=stt
                            status=' '
                                                
                        elif len(status)>1:
                            status = status
                            status=f' where  status in {status} '
                        elif len(status)==1:
                            status = tuple(status)
                            status = "('{}')".format(status[0])
                            status=f' where  status in {status}'
                        
                        timepar=' time_server '
                        ttm=' createtime '
                        st.sidebar.header("Tarixi seçin:")
                        date_typee=st.sidebar.radio("",("Ödəniş tarixi","İnkassasiya tarixi"),horizontal=True)
                        if date_typee=="Ödəniş tarixi":
                            date_type=' time_server '
                        else:
                            date_type=' g.collection_time '
                        a,b=st.sidebar.columns(2)     
                                
                        v=datetime.today().replace(day=1)
                        div=st.sidebar.container()
                        full_script=''
                        start_date=a.date_input("Başlanğıc tarix",v)
                        end_date=b.date_input("Son tarix")
                        f=a.checkbox("Filter")
                        st.sidebar.write(' ')
                        st.sidebar.write(' ')
                        c,d=st.sidebar.columns(2)
                        s2020=c.checkbox("2020")
                        s2021=d.checkbox("2021")
                        s2022=c.checkbox("2022")
                        s2023=d.checkbox("2023")
                        a=('Yanvar','Fevral','Mart','Aprel','May','İyun','İyul','Avqust','Sentyabr','Oktyabr','Noyabr','Dekabr')
                        full_script = '' 
                        if f:
                            start_date=start_date
                            end_date=end_date
                            ccc=f'''  {date_type} between '{start_date}' and '{end_date}' '''
                            if full_script:
                                full_script += ' or '
                            full_script += ccc
                        if s2020:
                            year=2020
                            ss2020=st.sidebar.multiselect('2020',a)
                        if s2021:
                            year=2021
                            ss2021=st.sidebar.multiselect('2021 ',a)
                        if s2022:
                            year=2022
                            ss2022=st.sidebar.multiselect('2022 ',a)
                        if s2023:
                            year=2023
                            ss2023=st.sidebar.multiselect('2023 ',a)
                        if s2020:
                            if ss2020!= ' ':
                                year=2020
                                #full_script = ''   
                                if 'Yanvar' in ss2020:
                                    jan_script=f''' {date_type} between \'{year}-01-01\' and \'{year}-02-01\' '''
                                    if full_script:
                                        full_script += ' or '
                                    full_script += jan_script
                                else :
                                    jan_script=' '
                                if 'Fevral' in ss2020:
                                    feb_script=f''' {date_type} between \'{year}-02-01\' and \'{year}-03-01\' '''
                                    if full_script:
                                        full_script += ' or '
                                    full_script += feb_script
                                else :
                                    feb_script=' '
                                if 'Mart' in ss2020:
                                    if full_script:
                                        full_script += ' or '
                                    mar_script=f''' {date_type} between \'{year}-03-01\' and \'{year}-04-01\' '''
                                    full_script += mar_script
                                else :
                                    mar_script=' '
                                if 'Aprel' in ss2020:
                                    if full_script:
                                        full_script += ' or '
                                    apr_script=f''' {date_type} between \'{year}-04-01\' and \'{year}-05-01\' '''
                                    full_script += apr_script
                                else :
                                    apr_script=' '
                                if 'May' in ss2020:
                                    if full_script:
                                        full_script += ' or '
                                    may_script=f''' {date_type} between \'{year}-05-01\' and \'{year}-06-01\' '''
                                    full_script += may_script
                                else :
                                    may_script=' '
                                if 'İyun' in ss2020:
                                    if full_script:
                                        full_script += ' or '
                                    jun_script=f''' {date_type} between \'{year}-06-01\' and \'{year}-07-01\' '''
                                    full_script += jun_script
                                else :
                                    jun_script=' '
                                if 'İyul' in ss2020:
                                    if full_script:
                                        full_script += ' or '
                                    jul_script=f''' {date_type} between \'{year}-07-01\' and \'{year}-08-01\' '''
                                    full_script += jul_script
                                else :
                                    jul_script=' '
                                if 'Avqust' in ss2020:
                                    if full_script:
                                        full_script += ' or '
                                    avg_script=f''' {date_type} between \'{year}-08-01\' and \'{year}-09-01\' '''
                                    full_script += avg_script
                                else :
                                    avg_script=' '
                                if 'Sentyabr' in ss2020:
                                    if full_script:
                                        full_script += ' or '
                                    sen_script=f''' {date_type} between \'{year}-09-01\' and \'{year}-10-01\' '''
                                    full_script += sen_script
                                else :
                                    sen_script=' '
                                if 'Oktyabr' in ss2020:
                                    if full_script:
                                        full_script += ' or '
                                    oct_script=f''' {date_type} between \'{year}-10-01\' and \'{year}-11-01\' '''
                                    full_script += oct_script
                                else :
                                    oct_script=' '
                                if 'Noyabr' in ss2020:
                                    if full_script:
                                        full_script += ' or '
                                    nov_script=f''' {date_type} between \'{year}-11-01\' and \'{year}-12-01\' '''
                                    full_script += nov_script
                                else :
                                    nov_script=' '
                                if 'Dekabr' in ss2020:
                                    if full_script:
                                        full_script += ' or '
                                    dec_script=f''' {date_type} between \'{year}-12-01\' and \'{year+1}-01-01\' '''
                                    full_script += dec_script
                                else :
                                    dec_script=' '
                        if s2021:
                            if ss2021!= ' ':
                                #full_script = ''  
                                year=2021 
                                if 'Yanvar' in ss2021:
                                    if full_script:
                                        full_script += ' or '
                                    jan_script=f''' {date_type} between \'{year}-01-01\' and \'{year}-02-01\' '''
                                    full_script += jan_script
                                else :
                                    jan_script=' '
                                if 'Fevral' in ss2021:
                                    feb_script=f''' {date_type} between \'{year}-02-01\' and \'{year}-03-01\' '''
                                    if full_script:
                                        full_script += ' or '
                                    full_script += feb_script
                                else :
                                    feb_script=' '
                                if 'Mart' in ss2021:
                                    if full_script:
                                        full_script += ' or '
                                    mar_script=f''' {date_type} between \'{year}-03-01\' and \'{year}-04-01\' '''
                                    full_script += mar_script
                                else :
                                    mar_script=' '
                                if 'Aprel' in ss2021:
                                    if full_script:
                                        full_script += ' or '
                                    apr_script=f''' {date_type} between \'{year}-04-01\' and \'{year}-05-01\' '''
                                    full_script += apr_script
                                else :
                                    apr_script=' '
                                if 'May' in ss2021:
                                    if full_script:
                                        full_script += ' or '
                                    may_script=f''' {date_type} between \'{year}-05-01\' and \'{year}-06-01\' '''
                                    full_script += may_script
                                else :
                                    may_script=' '
                                if 'İyun' in ss2021:
                                    if full_script:
                                        full_script += ' or '
                                    jun_script=f''' {date_type} between \'{year}-06-01\' and \'{year}-07-01\' '''
                                    full_script += jun_script
                                else :
                                    jun_script=' '
                                if 'İyul' in ss2021:
                                    if full_script:
                                        full_script += ' or '
                                    jul_script=f''' {date_type} between \'{year}-07-01\' and \'{year}-08-01\' '''
                                    full_script += jul_script
                                else :
                                    jul_script=' '
                                if 'Avqust' in ss2021:
                                    if full_script:
                                        full_script += ' or '
                                    avg_script=f''' {date_type} between \'{year}-08-01\' and \'{year}-09-01\' '''
                                    full_script += avg_script
                                else :
                                    avg_script=' '
                                if 'Sentyabr' in ss2021:
                                    if full_script:
                                        full_script += ' or '
                                    sen_script=f''' {date_type} between \'{year}-09-01\' and \'{year}-10-01\' '''
                                    full_script += sen_script
                                else :
                                    sen_script=' '
                                if 'Oktyabr' in ss2021:
                                    if full_script:
                                        full_script += ' or '
                                    oct_script=f''' {date_type} between \'{year}-10-01\' and \'{year}-11-01\' '''
                                    full_script += oct_script
                                else :
                                    oct_script=' '
                                if 'Noyabr' in ss2021:
                                    if full_script:
                                        full_script += ' or '
                                    nov_script=f''' {date_type} between \'{year}-11-01\' and \'{year}-12-01\' '''
                                    full_script += nov_script
                                else :
                                    nov_script=' '
                                if 'Dekabr' in ss2021:
                                    if full_script:
                                        full_script += ' or '
                                    dec_script=f''' {date_type} between \'{year}-12-01\' and \'{year+1}-01-01\' '''
                                    full_script += dec_script
                                else :
                                    dec_script=' '
                        if s2022:
                            if ss2022!= ' ':
                                #full_script = ''   
                                year=2022
                                if 'Yanvar' in ss2022:
                                    if full_script:
                                        full_script += ' or '
                                    jan_script=f''' {date_type} between \'{year}-01-01\' and \'{year}-02-01\' '''
                                    full_script += jan_script
                                else :
                                    jan_script=' '
                                if 'Fevral' in ss2022:
                                    feb_script=f''' {date_type} between \'{year}-02-01\' and \'{year}-03-01\' '''
                                    if full_script:
                                        full_script += ' or '
                                    full_script += feb_script
                                else :
                                    feb_script=' '
                                if 'Mart' in ss2022:
                                    if full_script:
                                        full_script += ' or '
                                    mar_script=f''' {date_type} between \'{year}-03-01\' and \'{year}-04-01\' '''
                                    full_script += mar_script
                                else :
                                    mar_script=' '
                                if 'Aprel' in ss2022:
                                    if full_script:
                                        full_script += ' or '
                                    apr_script=f''' {date_type} between \'{year}-04-01\' and \'{year}-05-01\' '''
                                    full_script += apr_script
                                else :
                                    apr_script=' '
                                if 'May' in ss2022:
                                    if full_script:
                                        full_script += ' or '
                                    may_script=f''' {date_type} between \'{year}-05-01\' and \'{year}-06-01\' '''
                                    full_script += may_script
                                else :
                                    may_script=' '
                                if 'İyun' in ss2022:
                                    if full_script:
                                        full_script += ' or '
                                    jun_script=f''' {date_type} between \'{year}-06-01\' and \'{year}-07-01\' '''
                                    full_script += jun_script
                                else :
                                    jun_script=' '
                                if 'İyul' in ss2022:
                                    if full_script:
                                        full_script += ' or '
                                    jul_script=f''' {date_type} between \'{year}-07-01\' and \'{year}-08-01\' '''
                                    full_script += jul_script
                                else :
                                    jul_script=' '
                                if 'Avqust' in ss2022:
                                    if full_script:
                                        full_script += ' or '
                                    avg_script=f''' {date_type} between \'{year}-08-01\' and \'{year}-09-01\' '''
                                    full_script += avg_script
                                else :
                                    avg_script=' '
                                if 'Sentyabr' in ss2022:
                                    if full_script:
                                        full_script += ' or '
                                    sen_script=f''' {date_type} between \'{year}-09-01\' and \'{year}-10-01\' '''
                                    full_script += sen_script
                                else :
                                    sen_script=' '
                                if 'Oktyabr' in ss2022:
                                    if full_script:
                                        full_script += ' or '
                                    oct_script=f''' {date_type} between \'{year}-10-01\' and \'{year}-11-01\' '''
                                    full_script += oct_script
                                else :
                                    oct_script=' '
                                if 'Noyabr' in ss2022:
                                    if full_script:
                                        full_script += ' or '
                                    nov_script=f''' {date_type} between \'{year}-11-01\' and \'{year}-12-01\' '''
                                    full_script += nov_script
                                else :
                                    nov_script=' '
                                if 'Dekabr' in ss2022:
                                    if full_script:
                                        full_script += ' or '
                                    dec_script=f''' {date_type} between \'{year}-12-01\' and \'{year+1}-01-01\' '''
                                    full_script += dec_script
                                else :
                                    dec_script=' '
                        if s2023:
                            if ss2023!= ' ':
                                        #full_script = ''   
                                year=2023
                                if 'Yanvar' in ss2023:
                                    if full_script:
                                        full_script += ' or '
                                    jan_script=f''' {date_type} between \'{year}-01-01\' and \'{year}-02-01\' '''
                                    full_script += jan_script
                                else :
                                    jan_script=' '
                                if 'Fevral' in ss2023:
                                    feb_script=f''' {date_type} between \'{year}-02-01\' and \'{year}-03-01\' '''
                                    if full_script:
                                        full_script += ' or '
                                    full_script += feb_script
                                else :
                                    feb_script=' '
                                if 'Mart' in ss2023:
                                    if full_script:
                                        full_script += ' or '
                                    mar_script=f''' {date_type} between \'{year}-03-01\' and \'{year}-04-01\' '''
                                    full_script += mar_script
                                else :
                                    mar_script=' '
                                if 'Aprel' in ss2023:
                                    if full_script:
                                        full_script += ' or '
                                    apr_script=f''' {date_type} between \'{year}-04-01\' and \'{year}-05-01\' '''
                                    full_script += apr_script
                                else :
                                    apr_script=' '
                                if 'May' in ss2023:
                                    if full_script:
                                        full_script += ' or '
                                    may_script=f''' {date_type} between \'{year}-05-01\' and \'{year}-06-01\' '''
                                    full_script += may_script
                                else :
                                    may_script=' '
                                if 'İyun' in ss2023:
                                    if full_script:
                                        full_script += ' or '
                                    jun_script=f''' {date_type} between \'{year}-06-01\' and \'{year}-07-01\' '''
                                    full_script += jun_script
                                else :
                                    jun_script=' '
                                if 'İyul' in ss2023:
                                    if full_script:
                                        full_script += ' or '
                                    jul_script=f''' {date_type} between \'{year}-07-01\' and \'{year}-08-01\' '''
                                    full_script += jul_script
                                else :
                                    jul_script=' '
                                if 'Avqust' in ss2023:
                                    if full_script:
                                        full_script += ' or '
                                    avg_script=f''' {date_type} between \'{year}-08-01\' and \'{year}-09-01\' '''
                                    full_script += avg_script
                                else :
                                    avg_script=' '
                                if 'Sentyabr' in ss2023:
                                    if full_script:
                                        full_script += ' or '
                                    sen_script=f''' {date_type} between \'{year}-09-01\' and \'{year}-10-01\' '''
                                    full_script += sen_script
                                else :
                                    sen_script=' '
                                if 'Oktyabr' in ss2023:
                                    if full_script:
                                        full_script += ' or '
                                    oct_script=f''' {date_type} between \'{year}-10-01\' and \'{year}-11-01\' '''
                                    full_script += oct_script
                                else :
                                    oct_script=' '
                                if 'Noyabr' in ss2023:
                                    if full_script:
                                        full_script += ' or '
                                    nov_script=f''' {date_type} between \'{year}-11-01\' and \'{year}-12-01\' '''
                                    full_script += nov_script
                                else :
                                    nov_script=' '
                                if 'Dekabr' in ss2023:
                                    if full_script:
                                        full_script += ' or '
                                    dec_script=f''' {date_type} between \'{year}-12-01\' and \'{year+1}-01-01\' '''
                                    full_script += dec_script
                                else :
                                    dec_script=' '
                                

                                
                        ss,dd=st.sidebar.columns(2)
                        last_fifteen=ss.checkbox("Son 15 gün")
                        last_thirty=dd.checkbox("Son 30 gün")
                        all_column=(
'id_operation','id_point', "Provider",  "Service", 'time_server', 'time_process',  'account', 'account2',  'status',  'sum_income' , 
 'sum_outcome' , "comment", 'id_money_collection', 'collection_time', 'cashcount', "1M", "5M", "10M", "20M", "50M", "100M", "200M", 'provider_trans', 'company'
)
                        defaultt=('id_operation','id_point', "Provider",  "Service", 'time_server', 'time_process',  'account', 'account2',  'status',  'sum_income' , 
 'sum_outcome' ,'cashcount', "1M", "5M", "10M", "20M", "50M", "100M", "200M", 'company')
                            
                        column_choice=st.sidebar.multiselect(" ",all_column,default=defaultt)

                        column_choice=tuple(column_choice)
                        column_choice='","'.join(column_choice)
                        column_choice='"'+column_choice+'"'
                        cf=st.sidebar.button("Filterləri təmizlə")
                                
                        if  last_fifteen:
                            if full_script:
                                full_script += ' or '
                            start_date,end_date=funcc.getlast15date()
                            lst15=f'''{date_type} between  \'{start_date}\' and \'{end_date}\''''
                            full_script += lst15
                        else :
                            last_fifteen=False
                            lst15=False
                                    #st.write(start_date,end_date)
                        if  last_thirty:
                            if full_script:
                                full_script += ' or '
                            start_date,end_date=funcc.getlast30date()
                            lst30=f'''{date_type} between  \'{start_date}\' and \'{end_date}\''''
                            full_script += lst30
                        else: 
                            last_thirty=False
                            lst30=False
                                
                        if  cf == False :
                            cf=False
                            clrfltr=' '
                        else :
                            start_date,end_date=funcc.getdate()
                                    #st.write(start_date,end_date)
                            full_script=f''' {date_type} between  \'{start_date}\' and \'{end_date}\' ''' 
                            lst30=True  
                            lst15=True
                        if full_script:
                            scripts = f" ({full_script.strip()})"
                        else :
                            start_date,end_date=funcc.getdate()
                            #st.write(start_date,end_date)
                            scripts=f''' {date_type} between  \'{start_date}\' and \'{end_date}\' '''
                        
                        if date_typee=="Ödəniş tarixi":
                            sdas=scripts.replace('time_server','createtime')
                        else :
                            sdas=scripts.replace('collection_time','cashoutdate ')
                        
                        pointmod=pointmpay.replace('id_point','pointid')
                            
                        a4=st.sidebar.button("Yarat")
                        if a4:
                                    
                            vv='Report '
                            st.header (vv)
                                        
                                                                    
                            SQl_query_filtered=all_transactions(column_choice,scripts,sdas,mp_pr,mp_sr,pointmpay,pointmod,status)
                            SQl_query_filtered = SQl_query_filtered.fillna('')
                                    
                                                        
                                    
                                        
                            a=pd.DataFrame(SQl_query_filtered)
                            numeric_cols=['sum_income','sum_outcome']
                                                
                            result = pd.DataFrame([{col: int(a[col].sum()) for col in numeric_cols}]).T
                                                
                            result = result.reset_index(drop=True).T
                            result.columns=['sum_income','sum_outcome']
                            result = result.reset_index()
                                                
                            result.columns=[' ','sum(sum_income)','sum(sum_outcome)']
                            result["count"]=pd.DataFrame(a.count()).iloc[0,0]
                            result.iloc[0,0]='Total'
                                    #result = result.set_index('Total')
                            funcc.dataframee1(result)
                            funcc.dataframee(SQl_query_filtered)
        
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
                      
                
                    
                    
                    
                
                
            elif selected3=='Maliyyə':
                company=st.sidebar.radio("",("Mpay","Modenis","All"),horizontal=True)
                
                if company=='Mpay': 
                    timepar=' time_server '
                    collection_time=' collection_time '
                
                else : 
                    timepar=' createtime ' 
                    collection_time=' cashoutdate '
                year_choices=st.sidebar.checkbox("İl") 
                current_year=datetime.now().year
                next_year=datetime.now().year+1
                current_month=datetime.now().month
                
                    
                
                #b=st.checkbox('Report')
                modeniss_container =st.container() 
                modeniss_g_container =st.container()
                mpayy_container =st.container() 
                mpayy_g_container =st.container()
                pr,sr,pointid_mp,pr_mod,ser_mod,point_mod,servicename=getdata(conn)   
                provider_name = pr["providername"]
                service_name_mpay=sr["servicename"]
                pointid_mpay=pointid_mp["id_point"]
                prid_mpay=pr["id_provider"] 
                provider_name_mod = pr_mod["nomenclaturename"]
                servicename=servicename[servicename['worksercervideid'].notnull()]
                prrname=np.unique(servicename["Provider yeni adlar"])
                
                point_mod=point_mod["pointid"] 
                if  year_choices==False:
                    if company=='All':
                        pr,sr,pointid_mp,pr_mod,ser_mod,point_mod,servicename=getdata(conn) 
                        sdd,point=dfg(conn)
                
                        prrname=np.unique(sdd["Provider yeni adlar"])
                    
                        #prrname=prrname.sort()
                        #st.write(prrname)
                        prras = st.sidebar.multiselect('Provider',prrname)
                        if prras !=[]:
                            prname=sdd[sdd["Provider yeni adlar"].isin(prras)]
                            #st.write(prname)
                            if len(prras)==1:
                                prras     = tuple(prras)
                                prras     = "('{}')".format(prras    [0]) 
                                mp_pr=f''' and  replace ( replace ( "Provider yeni adlar",'[MP] ','' ),'[MOD] ','' ) in {prras}'''
                                
                            elif len(prras)>1 :
                                mp_pr=f''' and  replace ( replace ( "Provider yeni adlar",'[MP] ','' ),'[MOD] ','' )in {tuple(prras)}'''
                            
                        else :
                            prname=servicename
                            mp_pr=' '
                        sr_name=np.unique(prname["Service name yeni adlar"])
                        serv = st.sidebar.multiselect('Xidmət',sr_name)
                        if serv==[]:
                            serv=sr_name
                            mp_sr=' '
                        elif len(serv)==1:
                            serv     = tuple(serv)
                            serv     = "('{}')".format(serv    [0]) 
                            mp_sr=f' and "Service" in {serv} '
                        else:
                            mp_sr=f' and "Service" in {tuple(serv)} '
                        #point=np.unique(point["pointid"])
                        
                        #st.write(point)
                        pointmpay=st.sidebar.multiselect("Pointid",point)    
                        
                        if pointmpay==[]:
                            pointmpay=pointmpay
                            pointmpay=' '
                        elif len(pointmpay)==1:
                            pointmpay     = tuple(pointmpay)
                            pointmpay     = "('{}')".format(pointmpay    [0]) 
                            pointmpay=f' and id_point in {pointmpay} '
                        else:
                            pointmpay=f' and id_point in {tuple(pointmpay)} '
                        new_row=pd.Series(['All'])
                        prrname=np.unique(point["pointid"])
                        #pointidd_mpay = pointid_mpay.append(new_row, ignore_index=True)
                        stt=('Success', 'Revoked','Processing', 'Success/Cancelled ','Success/Funds return ', 'Success/Corrected ', 'Success/Blocked ', 'Error/Cancelled by user', 'Error/Cancelled by support', 'Error/Rejected by provider', 'Error/Corrected', 'Error/Uncorrectable error', 'Error/Revocated', 'Error/Blocked by user', 'Processing/Provider determined', 'Processing/Processing', 'Processing/Unknown', 'Processing/Picked up', 'Processing/Error', 'Processing/In wait','Processing/Confirm' )
                        status=st.sidebar.multiselect("Status",stt,default="Success",help='Bütün statusları seçmək üçün heç bir seçim etməyin')
                        status=tuple(status)
                        if status ==():
                            status=stt
                            status=' '
                                                
                        elif len(status)>1:
                            status = status
                            status=f' where  status in {status} '
                        elif len(status)==1:
                            status = tuple(status)
                            status = "('{}')".format(status[0])
                            status=f' where  status in {status}'
                        
                        timepar=' time_server '
                        ttm=' createtime '
                        st.sidebar.header("Tarixi seçin:")
                        date_typee=st.sidebar.radio("",("Ödəniş tarixi","İnkassasiya tarixi"),horizontal=True)
                        if date_typee=="Ödəniş tarixi":
                            date_type=' time_server '
                        else:
                            date_type=' collection_time '
                        a,b=st.sidebar.columns(2)     
                                
                        v=datetime.today().replace(day=1)
                        div=st.sidebar.container()
                        full_script=''
                        start_date=a.date_input("Başlanğıc tarix",v)
                        end_date=b.date_input("Son tarix")
                        f=a.checkbox("Filter")
                        st.sidebar.write(' ')
                        st.sidebar.write(' ')
                        c,d=st.sidebar.columns(2)
                        s2020=c.checkbox("2020")
                        s2021=d.checkbox("2021")
                        s2022=c.checkbox("2022")
                        s2023=d.checkbox("2023")
                        a=('Yanvar','Fevral','Mart','Aprel','May','İyun','İyul','Avqust','Sentyabr','Oktyabr','Noyabr','Dekabr')
                        full_script = '' 
                        if f:
                            start_date=start_date
                            end_date=end_date
                            ccc=f'''  {date_type} between '{start_date}' and '{end_date}' '''
                            if full_script:
                                full_script += ' or '
                            full_script += ccc
                        if s2020:
                            year=2020
                            ss2020=st.sidebar.multiselect('2020',a)
                        if s2021:
                            year=2021
                            ss2021=st.sidebar.multiselect('2021 ',a)
                        if s2022:
                            year=2022
                            ss2022=st.sidebar.multiselect('2022 ',a)
                        if s2023:
                            year=2023
                            ss2023=st.sidebar.multiselect('2023 ',a)
                        if s2020:
                            if ss2020!= ' ':
                                year=2020
                                #full_script = ''   
                                if 'Yanvar' in ss2020:
                                    jan_script=f''' {date_type} between \'{year}-01-01\' and \'{year}-02-01\' '''
                                    if full_script:
                                        full_script += ' or '
                                    full_script += jan_script
                                else :
                                    jan_script=' '
                                if 'Fevral' in ss2020:
                                    feb_script=f''' {date_type} between \'{year}-02-01\' and \'{year}-03-01\' '''
                                    if full_script:
                                        full_script += ' or '
                                    full_script += feb_script
                                else :
                                    feb_script=' '
                                if 'Mart' in ss2020:
                                    if full_script:
                                        full_script += ' or '
                                    mar_script=f''' {date_type} between \'{year}-03-01\' and \'{year}-04-01\' '''
                                    full_script += mar_script
                                else :
                                    mar_script=' '
                                if 'Aprel' in ss2020:
                                    if full_script:
                                        full_script += ' or '
                                    apr_script=f''' {date_type} between \'{year}-04-01\' and \'{year}-05-01\' '''
                                    full_script += apr_script
                                else :
                                    apr_script=' '
                                if 'May' in ss2020:
                                    if full_script:
                                        full_script += ' or '
                                    may_script=f''' {date_type} between \'{year}-05-01\' and \'{year}-06-01\' '''
                                    full_script += may_script
                                else :
                                    may_script=' '
                                if 'İyun' in ss2020:
                                    if full_script:
                                        full_script += ' or '
                                    jun_script=f''' {date_type} between \'{year}-06-01\' and \'{year}-07-01\' '''
                                    full_script += jun_script
                                else :
                                    jun_script=' '
                                if 'İyul' in ss2020:
                                    if full_script:
                                        full_script += ' or '
                                    jul_script=f''' {date_type} between \'{year}-07-01\' and \'{year}-08-01\' '''
                                    full_script += jul_script
                                else :
                                    jul_script=' '
                                if 'Avqust' in ss2020:
                                    if full_script:
                                        full_script += ' or '
                                    avg_script=f''' {date_type} between \'{year}-08-01\' and \'{year}-09-01\' '''
                                    full_script += avg_script
                                else :
                                    avg_script=' '
                                if 'Sentyabr' in ss2020:
                                    if full_script:
                                        full_script += ' or '
                                    sen_script=f''' {date_type} between \'{year}-09-01\' and \'{year}-10-01\' '''
                                    full_script += sen_script
                                else :
                                    sen_script=' '
                                if 'Oktyabr' in ss2020:
                                    if full_script:
                                        full_script += ' or '
                                    oct_script=f''' {date_type} between \'{year}-10-01\' and \'{year}-11-01\' '''
                                    full_script += oct_script
                                else :
                                    oct_script=' '
                                if 'Noyabr' in ss2020:
                                    if full_script:
                                        full_script += ' or '
                                    nov_script=f''' {date_type} between \'{year}-11-01\' and \'{year}-12-01\' '''
                                    full_script += nov_script
                                else :
                                    nov_script=' '
                                if 'Dekabr' in ss2020:
                                    if full_script:
                                        full_script += ' or '
                                    dec_script=f''' {date_type} between \'{year}-12-01\' and \'{year+1}-01-01\' '''
                                    full_script += dec_script
                                else :
                                    dec_script=' '
                        if s2021:
                            if ss2021!= ' ':
                                #full_script = ''  
                                year=2021 
                                if 'Yanvar' in ss2021:
                                    if full_script:
                                        full_script += ' or '
                                    jan_script=f''' {date_type} between \'{year}-01-01\' and \'{year}-02-01\' '''
                                    full_script += jan_script
                                else :
                                    jan_script=' '
                                if 'Fevral' in ss2021:
                                    feb_script=f''' {date_type} between \'{year}-02-01\' and \'{year}-03-01\' '''
                                    if full_script:
                                        full_script += ' or '
                                    full_script += feb_script
                                else :
                                    feb_script=' '
                                if 'Mart' in ss2021:
                                    if full_script:
                                        full_script += ' or '
                                    mar_script=f''' {date_type} between \'{year}-03-01\' and \'{year}-04-01\' '''
                                    full_script += mar_script
                                else :
                                    mar_script=' '
                                if 'Aprel' in ss2021:
                                    if full_script:
                                        full_script += ' or '
                                    apr_script=f''' {date_type} between \'{year}-04-01\' and \'{year}-05-01\' '''
                                    full_script += apr_script
                                else :
                                    apr_script=' '
                                if 'May' in ss2021:
                                    if full_script:
                                        full_script += ' or '
                                    may_script=f''' {date_type} between \'{year}-05-01\' and \'{year}-06-01\' '''
                                    full_script += may_script
                                else :
                                    may_script=' '
                                if 'İyun' in ss2021:
                                    if full_script:
                                        full_script += ' or '
                                    jun_script=f''' {date_type} between \'{year}-06-01\' and \'{year}-07-01\' '''
                                    full_script += jun_script
                                else :
                                    jun_script=' '
                                if 'İyul' in ss2021:
                                    if full_script:
                                        full_script += ' or '
                                    jul_script=f''' {date_type} between \'{year}-07-01\' and \'{year}-08-01\' '''
                                    full_script += jul_script
                                else :
                                    jul_script=' '
                                if 'Avqust' in ss2021:
                                    if full_script:
                                        full_script += ' or '
                                    avg_script=f''' {date_type} between \'{year}-08-01\' and \'{year}-09-01\' '''
                                    full_script += avg_script
                                else :
                                    avg_script=' '
                                if 'Sentyabr' in ss2021:
                                    if full_script:
                                        full_script += ' or '
                                    sen_script=f''' {date_type} between \'{year}-09-01\' and \'{year}-10-01\' '''
                                    full_script += sen_script
                                else :
                                    sen_script=' '
                                if 'Oktyabr' in ss2021:
                                    if full_script:
                                        full_script += ' or '
                                    oct_script=f''' {date_type} between \'{year}-10-01\' and \'{year}-11-01\' '''
                                    full_script += oct_script
                                else :
                                    oct_script=' '
                                if 'Noyabr' in ss2021:
                                    if full_script:
                                        full_script += ' or '
                                    nov_script=f''' {date_type} between \'{year}-11-01\' and \'{year}-12-01\' '''
                                    full_script += nov_script
                                else :
                                    nov_script=' '
                                if 'Dekabr' in ss2021:
                                    if full_script:
                                        full_script += ' or '
                                    dec_script=f''' {date_type} between \'{year}-12-01\' and \'{year+1}-01-01\' '''
                                    full_script += dec_script
                                else :
                                    dec_script=' '
                        if s2022:
                            if ss2022!= ' ':
                                #full_script = ''   
                                year=2022
                                if 'Yanvar' in ss2022:
                                    if full_script:
                                        full_script += ' or '
                                    jan_script=f''' {date_type} between \'{year}-01-01\' and \'{year}-02-01\' '''
                                    full_script += jan_script
                                else :
                                    jan_script=' '
                                if 'Fevral' in ss2022:
                                    feb_script=f''' {date_type} between \'{year}-02-01\' and \'{year}-03-01\' '''
                                    if full_script:
                                        full_script += ' or '
                                    full_script += feb_script
                                else :
                                    feb_script=' '
                                if 'Mart' in ss2022:
                                    if full_script:
                                        full_script += ' or '
                                    mar_script=f''' {date_type} between \'{year}-03-01\' and \'{year}-04-01\' '''
                                    full_script += mar_script
                                else :
                                    mar_script=' '
                                if 'Aprel' in ss2022:
                                    if full_script:
                                        full_script += ' or '
                                    apr_script=f''' {date_type} between \'{year}-04-01\' and \'{year}-05-01\' '''
                                    full_script += apr_script
                                else :
                                    apr_script=' '
                                if 'May' in ss2022:
                                    if full_script:
                                        full_script += ' or '
                                    may_script=f''' {date_type} between \'{year}-05-01\' and \'{year}-06-01\' '''
                                    full_script += may_script
                                else :
                                    may_script=' '
                                if 'İyun' in ss2022:
                                    if full_script:
                                        full_script += ' or '
                                    jun_script=f''' {date_type} between \'{year}-06-01\' and \'{year}-07-01\' '''
                                    full_script += jun_script
                                else :
                                    jun_script=' '
                                if 'İyul' in ss2022:
                                    if full_script:
                                        full_script += ' or '
                                    jul_script=f''' {date_type} between \'{year}-07-01\' and \'{year}-08-01\' '''
                                    full_script += jul_script
                                else :
                                    jul_script=' '
                                if 'Avqust' in ss2022:
                                    if full_script:
                                        full_script += ' or '
                                    avg_script=f''' {date_type} between \'{year}-08-01\' and \'{year}-09-01\' '''
                                    full_script += avg_script
                                else :
                                    avg_script=' '
                                if 'Sentyabr' in ss2022:
                                    if full_script:
                                        full_script += ' or '
                                    sen_script=f''' {date_type} between \'{year}-09-01\' and \'{year}-10-01\' '''
                                    full_script += sen_script
                                else :
                                    sen_script=' '
                                if 'Oktyabr' in ss2022:
                                    if full_script:
                                        full_script += ' or '
                                    oct_script=f''' {date_type} between \'{year}-10-01\' and \'{year}-11-01\' '''
                                    full_script += oct_script
                                else :
                                    oct_script=' '
                                if 'Noyabr' in ss2022:
                                    if full_script:
                                        full_script += ' or '
                                    nov_script=f''' {date_type} between \'{year}-11-01\' and \'{year}-12-01\' '''
                                    full_script += nov_script
                                else :
                                    nov_script=' '
                                if 'Dekabr' in ss2022:
                                    if full_script:
                                        full_script += ' or '
                                    dec_script=f''' {date_type} between \'{year}-12-01\' and \'{year+1}-01-01\' '''
                                    full_script += dec_script
                                else :
                                    dec_script=' '
                        if s2023:
                            if ss2023!= ' ':
                                        #full_script = ''   
                                year=2023
                                if 'Yanvar' in ss2023:
                                    if full_script:
                                        full_script += ' or '
                                    jan_script=f''' {date_type} between \'{year}-01-01\' and \'{year}-02-01\' '''
                                    full_script += jan_script
                                else :
                                    jan_script=' '
                                if 'Fevral' in ss2023:
                                    feb_script=f''' {date_type} between \'{year}-02-01\' and \'{year}-03-01\' '''
                                    if full_script:
                                        full_script += ' or '
                                    full_script += feb_script
                                else :
                                    feb_script=' '
                                if 'Mart' in ss2023:
                                    if full_script:
                                        full_script += ' or '
                                    mar_script=f''' {date_type} between \'{year}-03-01\' and \'{year}-04-01\' '''
                                    full_script += mar_script
                                else :
                                    mar_script=' '
                                if 'Aprel' in ss2023:
                                    if full_script:
                                        full_script += ' or '
                                    apr_script=f''' {date_type} between \'{year}-04-01\' and \'{year}-05-01\' '''
                                    full_script += apr_script
                                else :
                                    apr_script=' '
                                if 'May' in ss2023:
                                    if full_script:
                                        full_script += ' or '
                                    may_script=f''' {date_type} between \'{year}-05-01\' and \'{year}-06-01\' '''
                                    full_script += may_script
                                else :
                                    may_script=' '
                                if 'İyun' in ss2023:
                                    if full_script:
                                        full_script += ' or '
                                    jun_script=f''' {date_type} between \'{year}-06-01\' and \'{year}-07-01\' '''
                                    full_script += jun_script
                                else :
                                    jun_script=' '
                                if 'İyul' in ss2023:
                                    if full_script:
                                        full_script += ' or '
                                    jul_script=f''' {date_type} between \'{year}-07-01\' and \'{year}-08-01\' '''
                                    full_script += jul_script
                                else :
                                    jul_script=' '
                                if 'Avqust' in ss2023:
                                    if full_script:
                                        full_script += ' or '
                                    avg_script=f''' {date_type} between \'{year}-08-01\' and \'{year}-09-01\' '''
                                    full_script += avg_script
                                else :
                                    avg_script=' '
                                if 'Sentyabr' in ss2023:
                                    if full_script:
                                        full_script += ' or '
                                    sen_script=f''' {date_type} between \'{year}-09-01\' and \'{year}-10-01\' '''
                                    full_script += sen_script
                                else :
                                    sen_script=' '
                                if 'Oktyabr' in ss2023:
                                    if full_script:
                                        full_script += ' or '
                                    oct_script=f''' {date_type} between \'{year}-10-01\' and \'{year}-11-01\' '''
                                    full_script += oct_script
                                else :
                                    oct_script=' '
                                if 'Noyabr' in ss2023:
                                    if full_script:
                                        full_script += ' or '
                                    nov_script=f''' {date_type} between \'{year}-11-01\' and \'{year}-12-01\' '''
                                    full_script += nov_script
                                else :
                                    nov_script=' '
                                if 'Dekabr' in ss2023:
                                    if full_script:
                                        full_script += ' or '
                                    dec_script=f''' {date_type} between \'{year}-12-01\' and \'{year+1}-01-01\' '''
                                    full_script += dec_script
                                else :
                                    dec_script=' '
                                

                                
                        ss,dd=st.sidebar.columns(2)
                        last_fifteen=ss.checkbox("Son 15 gün")
                        last_thirty=dd.checkbox("Son 30 gün")
                        all_column=(
'id_operation','id_point', "Provider",  "Service", 'time_server', 'time_process',  'account', 'account2',  'status',  'sum_income' , 
 'sum_outcome' , "comment", 'id_money_collection', 'collection_time', 'cashcount', "1M", "5M", "10M", "20M", "50M", "100M", "200M", 'provider_trans', 'company'
)
                        defaultt=('id_operation','id_point', "Provider",  "Service", 'time_server', 'time_process',  'account', 'account2',  'status',  'sum_income' , 
 'sum_outcome' ,'cashcount', "1M", "5M", "10M", "20M", "50M", "100M", "200M", 'company')
                            
                        column_choice=st.sidebar.multiselect(" ",all_column,default=defaultt)

                        column_choice=tuple(column_choice)
                        column_choice='","'.join(column_choice)
                        column_choice='"'+column_choice+'"'
                        cf=st.sidebar.button("Filterləri təmizlə")
                                
                        if  last_fifteen:
                            if full_script:
                                full_script += ' or '
                            start_date,end_date=funcc.getlast15date()
                            lst15=f'''{date_type} between  \'{start_date}\' and \'{end_date}\''''
                            full_script += lst15
                        else :
                            last_fifteen=False
                            lst15=False
                                    #st.write(start_date,end_date)
                        if  last_thirty:
                            if full_script:
                                full_script += ' or '
                            start_date,end_date=funcc.getlast30date()
                            lst30=f'''{date_type} between  \'{start_date}\' and \'{end_date}\''''
                            full_script += lst30
                        else: 
                            last_thirty=False
                            lst30=False
                                
                        if  cf == False :
                            cf=False
                            clrfltr=' '
                        else :
                            start_date,end_date=funcc.getdate()
                                    #st.write(start_date,end_date)
                            full_script=f''' {date_type} between  \'{start_date}\' and \'{end_date}\' ''' 
                            lst30=True  
                            lst15=True
                        if full_script:
                            scripts = f" ({full_script.strip()})"
                        else :
                            start_date,end_date=funcc.getdate()
                            #st.write(start_date,end_date)
                            scripts=f''' {date_type} between  \'{start_date}\' and \'{end_date}\' '''
                        
                        if date_typee=="Ödəniş tarixi":
                            sdas=scripts.replace('time_server','createtime')
                        else :
                            sdas=scripts.replace('collection_time','cashoutdate ')
                        
                        pointmod=pointmpay.replace('id_point','pointid')
                        
                        a4=st.sidebar.button("Yarat")
                        if a4:
                                    
                            vv='Report '
                            st.header (vv)
                                        
                                                                    
                            SQl_query_filtered=all_transactions(column_choice,scripts,sdas,mp_pr,mp_sr,pointmpay,pointmod,status)
                            SQl_query_filtered = SQl_query_filtered.fillna('')
                                    
                                                        
                                    
                                        
                            a=pd.DataFrame(SQl_query_filtered)
                            numeric_cols=['sum_income','sum_outcome']
                                                
                            result = pd.DataFrame([{col: int(a[col].sum()) for col in numeric_cols}]).T
                                                
                            result = result.reset_index(drop=True).T
                            result.columns=['sum_income','sum_outcome']
                            result = result.reset_index()
                                                
                            result.columns=[' ','sum(sum_income)','sum(sum_outcome)']
                            result["count"]=pd.DataFrame(a.count()).iloc[0,0]
                            result.iloc[0,0]='Total'
                                    #result = result.set_index('Total')
                            funcc.dataframee1(result)
                            funcc.dataframee(SQl_query_filtered)
                            
                            
                        else:
                            st.markdown(
    """
    <style>
        div [data-testid=stImage]{
            text-align: center;
            display: block;
            margin-left: auto;
            margin-right: auto;
            width: 100%;
            margin-top: 20%;
        }

    </style>
    """, unsafe_allow_html=True
)

                            icon=Image.open('edatamart.png')        
                            st.image(icon)    
                        dw=st.sidebar.checkbox("Yüklə")
                        if dw:
                            vv='Report '
                            SQl_query_filtered=all_transactions(column_choice,scripts,sdas,mp_pr,mp_sr,pointmpay,pointmod,status)
                            csv = funcc.convert_df(SQl_query_filtered)
                            st.sidebar.header("Yüklə:")
                            st.sidebar.download_button(label="CSV",data=csv,file_name=vv+'.csv',mime='text/csv'    )
                            st.sidebar.download_button(label="TEXT",data=csv,file_name=vv+'.txt',mime='text')
                                #excel=funcc.to_excel(SQl_query_filtered)
                            if len(SQl_query_filtered.index)>1000000:
                                cl=st.sidebar.checkbox("Excel")
                                if cl:
                                    excel=funcc.to_excell(SQl_query_filtered)
                                    st.sidebar.download_button(label="EXCEL",data=excel,file_name=vv+'.xlsx',mime='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
                            else:
                                excel=funcc.to_excel(SQl_query_filtered)
                                st.sidebar.download_button(label="EXCEL",data=excel,file_name=vv+'.xlsx',mime='xlsx')                            

                        
                            
                        
                            
                            
                            
                         
                    elif company=='Mpay':
                        #prras = st.sidebar.multiselect('df',prrname)
                        prras = st.sidebar.multiselect('Provider', prrname,help='Bütün providerləri seçmək üçün heç bir seçim etməyin')
                        if prras !=[]:
                            prname=servicename[servicename["Provider yeni adlar"].isin(prras)]
                            
                            if len(prras)==1:
                                prras     = tuple(prras)
                                prras     = "('{}')".format(prras    [0]) 
                                mp_pr=f' and "Provider" in {prras}'
                                
                            elif len(prras)>1 :
                                mp_pr=f' and "Provider" in {tuple(prras)}'
                            
                        else :
                            prname=servicename
                            mp_pr=' '
                        sr_name=np.unique(prname["Service name yeni adlar"])
                        serv = st.sidebar.multiselect('Xidmət',sr_name)
                        if serv==[]:
                            serv=sr_name
                            mp_sr=' '
                        elif len(serv)==1:
                            serv     = tuple(serv)
                            serv     = "('{}')".format(serv    [0]) 
                            mp_sr=f' and "Service" in {serv} '
                        else:
                            mp_sr=f' and "Service" in {tuple(serv)} '
                                
                        stt=('Success', 'Success/Cancelled ','Success/Funds return ', 'Success/Corrected ', 'Success/Blocked ', 'Error/Cancelled by user', 'Error/Cancelled by support', 'Error/Rejected by provider', 'Error/Corrected', 'Error/Uncorrectable error', 'Error/Revocated', 'Error/Blocked by user', 'Processing/Provider determined', 'Processing/Processing', 'Processing/Unknown', 'Processing/Picked up', 'Processing/Error', 'Processing/In wait','Processing/Confirm' )
                        status=st.sidebar.multiselect("Status",stt,default="Success",help='Bütün statusları seçmək üçün heç bir seçim etməyin')
                        status=tuple(status)
                        mpay_chashout=st.sidebar.text_input('İnkassasiya id')
                        if mpay_chashout!='':
                            mpay_ccount=f' and g.id_money_collection in ({mpay_chashout})'
                        else:
                            mpay_ccount=' '
                        
                        if status ==():
                            status=stt
                            status=' '
                                #st.write(status)
                                #new_row = {'id_point': 'All'}
                        elif len(status)>1:
                            status = status
                            status=f' and status in {status} '
                        elif len(status)==1:
                            status = tuple(status)
                            status = "('{}')".format(status[0])
                            status=f' and status in {status} '
                        
                           
                        new_row=pd.Series(['All'])
                        #pointidd_mpay = pointid_mpay.append(new_row, ignore_index=True)
                        pointidd_mpay=pd.concat([pointid_mpay,new_row])
                        pointmpay=st.sidebar.multiselect("Pointid",pointidd_mpay,default='All')
                        pointmpay=tuple(pointmpay)
                        if len(pointmpay)>1:
                            pointmpay = pointmpay
                        else:
                            pointmpay = tuple(pointmpay)
                            pointmpay = "('{}')".format(pointmpay[0])               
                        if 'All' in pointmpay:
                            mpay_point= ' '
                                    
                        else : 
                            mpay_point= f' and g.id_point in {pointmpay}  '
                        st.sidebar.header("Tarixi seçin:")
                        date_typee=st.sidebar.radio("",("Ödəniş tarixi","İnkassasiya tarixi"),horizontal=True)
                        if date_typee=="Ödəniş tarixi":
                            date_type=timepar
                        else:
                            date_type=' g.collection_time '
                        a,b=st.sidebar.columns(2)     
                        
                        v=datetime.today().replace(day=1)
                        div=st.sidebar.container()
                        full_script=''
                        start_date=a.date_input("Başlanğıc tarix",v)
                        end_date=b.date_input("Son tarix")
                        f=a.checkbox("Filter")
                        st.sidebar.write(' ')
                        st.sidebar.write(' ')
                        c,d=st.sidebar.columns(2)
                        s2020=c.checkbox("2020")
                        s2021=d.checkbox("2021")
                        s2022=c.checkbox("2022")
                        s2023=d.checkbox("2023")
                        a=('Yanvar','Fevral','Mart','Aprel','May','İyun','İyul','Avqust','Sentyabr','Oktyabr','Noyabr','Dekabr')
                        full_script = '' 
                        if f:
                            start_date=start_date
                            end_date=end_date
                            ccc=f'''  {date_type} between '{start_date}' and '{end_date}' '''
                            if full_script:
                                full_script += ' or '
                            full_script += ccc
                        if s2020:
                            year=2020
                            ss2020=st.sidebar.multiselect('2020',a)
                        if s2021:
                            year=2021
                            ss2021=st.sidebar.multiselect('2021 ',a)
                        if s2022:
                            year=2022
                            ss2022=st.sidebar.multiselect('2022 ',a)
                        if s2023:
                            year=2023
                            ss2023=st.sidebar.multiselect('2023 ',a)
                        if s2020:
                            if ss2020!= ' ':
                                year=2020
                                #full_script = ''   
                                if 'Yanvar' in ss2020:
                                    jan_script=f'''  {date_type} between \'{year}-01-01\' and \'{year}-02-01\' '''
                                    if full_script:
                                        full_script += ' or '
                                    full_script += jan_script
                                else :
                                    jan_script=' '
                                if 'Fevral' in ss2020:
                                    feb_script=f'''  {date_type} between \'{year}-02-01\' and \'{year}-03-01\' '''
                                    if full_script:
                                        full_script += ' or '
                                    full_script += feb_script
                                else :
                                    feb_script=' '
                                if 'Mart' in ss2020:
                                    if full_script:
                                        full_script += ' or '
                                    mar_script=f'''  {date_type} between \'{year}-03-01\' and \'{year}-04-01\' '''
                                    full_script += mar_script
                                else :
                                    mar_script=' '
                                if 'Aprel' in ss2020:
                                    if full_script:
                                        full_script += ' or '
                                    apr_script=f'''  {date_type} between \'{year}-04-01\' and \'{year}-05-01\' '''
                                    full_script += apr_script
                                else :
                                    apr_script=' '
                                if 'May' in ss2020:
                                    if full_script:
                                        full_script += ' or '
                                    may_script=f'''  {date_type} between \'{year}-05-01\' and \'{year}-06-01\' '''
                                    full_script += may_script
                                else :
                                    may_script=' '
                                if 'İyun' in ss2020:
                                    if full_script:
                                        full_script += ' or '
                                    jun_script=f'''  {date_type} between \'{year}-06-01\' and \'{year}-07-01\' '''
                                    full_script += jun_script
                                else :
                                    jun_script=' '
                                if 'İyul' in ss2020:
                                    if full_script:
                                        full_script += ' or '
                                    jul_script=f'''  {date_type} between \'{year}-07-01\' and \'{year}-08-01\' '''
                                    full_script += jul_script
                                else :
                                    jul_script=' '
                                if 'Avqust' in ss2020:
                                    if full_script:
                                        full_script += ' or '
                                    avg_script=f'''  {date_type} between \'{year}-08-01\' and \'{year}-09-01\' '''
                                    full_script += avg_script
                                else :
                                    avg_script=' '
                                if 'Sentyabr' in ss2020:
                                    if full_script:
                                        full_script += ' or '
                                    sen_script=f'''  {date_type} between \'{year}-09-01\' and \'{year}-10-01\' '''
                                    full_script += sen_script
                                else :
                                    sen_script=' '
                                if 'Oktyabr' in ss2020:
                                    if full_script:
                                        full_script += ' or '
                                    oct_script=f'''  {date_type} between \'{year}-10-01\' and \'{year}-11-01\' '''
                                    full_script += oct_script
                                else :
                                    oct_script=' '
                                if 'Noyabr' in ss2020:
                                    if full_script:
                                        full_script += ' or '
                                    nov_script=f'''  {date_type} between \'{year}-11-01\' and \'{year}-12-01\' '''
                                    full_script += nov_script
                                else :
                                    nov_script=' '
                                if 'Dekabr' in ss2020:
                                    if full_script:
                                        full_script += ' or '
                                    dec_script=f'''  {date_type} between \'{year}-12-01\' and \'{year+1}-01-01\' '''
                                    full_script += dec_script
                                else :
                                    dec_script=' '
                        if s2021:
                            if ss2021!= ' ':
                                #full_script = ''  
                                year=2021 
                                if 'Yanvar' in ss2021:
                                    if full_script:
                                        full_script += ' or '
                                    jan_script=f'''  {date_type} between \'{year}-01-01\' and \'{year}-02-01\' '''
                                    full_script += jan_script
                                else :
                                    jan_script=' '
                                if 'Fevral' in ss2021:
                                    feb_script=f'''  {date_type} between \'{year}-02-01\' and \'{year}-03-01\' '''
                                    if full_script:
                                        full_script += ' or '
                                    full_script += feb_script
                                else :
                                    feb_script=' '
                                if 'Mart' in ss2021:
                                    if full_script:
                                        full_script += ' or '
                                    mar_script=f'''  {date_type} between \'{year}-03-01\' and \'{year}-04-01\' '''
                                    full_script += mar_script
                                else :
                                    mar_script=' '
                                if 'Aprel' in ss2021:
                                    if full_script:
                                        full_script += ' or '
                                    apr_script=f'''  {date_type} between \'{year}-04-01\' and \'{year}-05-01\' '''
                                    full_script += apr_script
                                else :
                                    apr_script=' '
                                if 'May' in ss2021:
                                    if full_script:
                                        full_script += ' or '
                                    may_script=f'''  {date_type} between \'{year}-05-01\' and \'{year}-06-01\' '''
                                    full_script += may_script
                                else :
                                    may_script=' '
                                if 'İyun' in ss2021:
                                    if full_script:
                                        full_script += ' or '
                                    jun_script=f'''  {date_type} between \'{year}-06-01\' and \'{year}-07-01\' '''
                                    full_script += jun_script
                                else :
                                    jun_script=' '
                                if 'İyul' in ss2021:
                                    if full_script:
                                        full_script += ' or '
                                    jul_script=f'''  {date_type} between \'{year}-07-01\' and \'{year}-08-01\' '''
                                    full_script += jul_script
                                else :
                                    jul_script=' '
                                if 'Avqust' in ss2021:
                                    if full_script:
                                        full_script += ' or '
                                    avg_script=f'''  {date_type} between \'{year}-08-01\' and \'{year}-09-01\' '''
                                    full_script += avg_script
                                else :
                                    avg_script=' '
                                if 'Sentyabr' in ss2021:
                                    if full_script:
                                        full_script += ' or '
                                    sen_script=f'''  {date_type} between \'{year}-09-01\' and \'{year}-10-01\' '''
                                    full_script += sen_script
                                else :
                                    sen_script=' '
                                if 'Oktyabr' in ss2021:
                                    if full_script:
                                        full_script += ' or '
                                    oct_script=f'''  {date_type} between \'{year}-10-01\' and \'{year}-11-01\' '''
                                    full_script += oct_script
                                else :
                                    oct_script=' '
                                if 'Noyabr' in ss2021:
                                    if full_script:
                                        full_script += ' or '
                                    nov_script=f'''  {date_type} between \'{year}-11-01\' and \'{year}-12-01\' '''
                                    full_script += nov_script
                                else :
                                    nov_script=' '
                                if 'Dekabr' in ss2021:
                                    if full_script:
                                        full_script += ' or '
                                    dec_script=f'''  {date_type} between \'{year}-12-01\' and \'{year+1}-01-01\' '''
                                    full_script += dec_script
                                else :
                                    dec_script=' '
                        if s2022:
                            if ss2022!= ' ':
                                #full_script = ''   
                                year=2022
                                if 'Yanvar' in ss2022:
                                    if full_script:
                                        full_script += ' or '
                                    jan_script=f'''  {date_type} between \'{year}-01-01\' and \'{year}-02-01\' '''
                                    full_script += jan_script
                                else :
                                    jan_script=' '
                                if 'Fevral' in ss2022:
                                    feb_script=f'''  {date_type} between \'{year}-02-01\' and \'{year}-03-01\' '''
                                    if full_script:
                                        full_script += ' or '
                                    full_script += feb_script
                                else :
                                    feb_script=' '
                                if 'Mart' in ss2022:
                                    if full_script:
                                        full_script += ' or '
                                    mar_script=f'''  {date_type} between \'{year}-03-01\' and \'{year}-04-01\' '''
                                    full_script += mar_script
                                else :
                                    mar_script=' '
                                if 'Aprel' in ss2022:
                                    if full_script:
                                        full_script += ' or '
                                    apr_script=f'''  {date_type} between \'{year}-04-01\' and \'{year}-05-01\' '''
                                    full_script += apr_script
                                else :
                                    apr_script=' '
                                if 'May' in ss2022:
                                    if full_script:
                                        full_script += ' or '
                                    may_script=f'''  {date_type} between \'{year}-05-01\' and \'{year}-06-01\' '''
                                    full_script += may_script
                                else :
                                    may_script=' '
                                if 'İyun' in ss2022:
                                    if full_script:
                                        full_script += ' or '
                                    jun_script=f'''  {date_type} between \'{year}-06-01\' and \'{year}-07-01\' '''
                                    full_script += jun_script
                                else :
                                    jun_script=' '
                                if 'İyul' in ss2022:
                                    if full_script:
                                        full_script += ' or '
                                    jul_script=f'''  {date_type} between \'{year}-07-01\' and \'{year}-08-01\' '''
                                    full_script += jul_script
                                else :
                                    jul_script=' '
                                if 'Avqust' in ss2022:
                                    if full_script:
                                        full_script += ' or '
                                    avg_script=f'''  {date_type} between \'{year}-08-01\' and \'{year}-09-01\' '''
                                    full_script += avg_script
                                else :
                                    avg_script=' '
                                if 'Sentyabr' in ss2022:
                                    if full_script:
                                        full_script += ' or '
                                    sen_script=f'''  {date_type} between \'{year}-09-01\' and \'{year}-10-01\' '''
                                    full_script += sen_script
                                else :
                                    sen_script=' '
                                if 'Oktyabr' in ss2022:
                                    if full_script:
                                        full_script += ' or '
                                    oct_script=f'''  {date_type} between \'{year}-10-01\' and \'{year}-11-01\' '''
                                    full_script += oct_script
                                else :
                                    oct_script=' '
                                if 'Noyabr' in ss2022:
                                    if full_script:
                                        full_script += ' or '
                                    nov_script=f'''  {date_type} between \'{year}-11-01\' and \'{year}-12-01\' '''
                                    full_script += nov_script
                                else :
                                    nov_script=' '
                                if 'Dekabr' in ss2022:
                                    if full_script:
                                        full_script += ' or '
                                    dec_script=f'''  {date_type} between \'{year}-12-01\' and \'{year+1}-01-01\' '''
                                    full_script += dec_script
                                else :
                                    dec_script=' '
                        if s2023:
                            if ss2023!= ' ':
                                #full_script = ''   
                                year=2023
                                if 'Yanvar' in ss2023:
                                    if full_script:
                                        full_script += ' or '
                                    jan_script=f'''  {date_type} between \'{year}-01-01\' and \'{year}-02-01\' '''
                                    full_script += jan_script
                                else :
                                    jan_script=' '
                                if 'Fevral' in ss2023:
                                    feb_script=f'''  {date_type} between \'{year}-02-01\' and \'{year}-03-01\' '''
                                    if full_script:
                                        full_script += ' or '
                                    full_script += feb_script
                                else :
                                    feb_script=' '
                                if 'Mart' in ss2023:
                                    if full_script:
                                        full_script += ' or '
                                    mar_script=f'''  {date_type} between \'{year}-03-01\' and \'{year}-04-01\' '''
                                    full_script += mar_script
                                else :
                                    mar_script=' '
                                if 'Aprel' in ss2023:
                                    if full_script:
                                        full_script += ' or '
                                    apr_script=f'''  {date_type} between \'{year}-04-01\' and \'{year}-05-01\' '''
                                    full_script += apr_script
                                else :
                                    apr_script=' '
                                if 'May' in ss2023:
                                    if full_script:
                                        full_script += ' or '
                                    may_script=f'''  {date_type} between \'{year}-05-01\' and \'{year}-06-01\' '''
                                    full_script += may_script
                                else :
                                    may_script=' '
                                if 'İyun' in ss2023:
                                    if full_script:
                                        full_script += ' or '
                                    jun_script=f'''  {date_type} between \'{year}-06-01\' and \'{year}-07-01\' '''
                                    full_script += jun_script
                                else :
                                    jun_script=' '
                                if 'İyul' in ss2023:
                                    if full_script:
                                        full_script += ' or '
                                    jul_script=f'''  {date_type} between \'{year}-07-01\' and \'{year}-08-01\' '''
                                    full_script += jul_script
                                else :
                                    jul_script=' '
                                if 'Avqust' in ss2023:
                                    if full_script:
                                        full_script += ' or '
                                    avg_script=f'''  {date_type} between \'{year}-08-01\' and \'{year}-09-01\' '''
                                    full_script += avg_script
                                else :
                                    avg_script=' '
                                if 'Sentyabr' in ss2023:
                                    if full_script:
                                        full_script += ' or '
                                    sen_script=f'''  {date_type} between \'{year}-09-01\' and \'{year}-10-01\' '''
                                    full_script += sen_script
                                else :
                                    sen_script=' '
                                if 'Oktyabr' in ss2023:
                                    if full_script:
                                        full_script += ' or '
                                    oct_script=f'''  {date_type} between \'{year}-10-01\' and \'{year}-11-01\' '''
                                    full_script += oct_script
                                else :
                                    oct_script=' '
                                if 'Noyabr' in ss2023:
                                    if full_script:
                                        full_script += ' or '
                                    nov_script=f'''  {date_type} between \'{year}-11-01\' and \'{year}-12-01\' '''
                                    full_script += nov_script
                                else :
                                    nov_script=' '
                                if 'Dekabr' in ss2023:
                                    if full_script:
                                        full_script += ' or '
                                    dec_script=f'''  {date_type} between \'{year}-12-01\' and \'{year+1}-01-01\' '''
                                    full_script += dec_script
                                else :
                                    dec_script=' '
                        

                        
                        ss,dd=st.sidebar.columns(2)
                        last_fifteen=ss.checkbox("Son 15 gün")
                        last_thirty=dd.checkbox("Son 30 gün")
                        all_column=('id_operation', "Provider", "Service", "Agent", 'provider_trans', 'id_point', 'account', 'account2', 
                                        'status','time_server', 'time_process', 'sum_income', 'sum_outcome', 'cashcount', "1M", "5M", "10M", 
                                        "20M", "50M", "100M", "200M", 'id_money_collection', 'collection_time', "comment", 'TotalCashCount' ,
                                        'TotalAmount')
                        defaultt=('id_operation', "Provider", "Service", "Agent", 'id_point', 'account', 'account2', 
                                        'status','time_server', 'sum_income', 'sum_outcome', 'cashcount', "1M", "5M", "10M", 
                                        "20M", "50M", "100M", "200M")
                            
                        column_choice=st.sidebar.multiselect(" ",all_column,default=defaultt)

                        column_choice=tuple(column_choice)
                        column_choice='","'.join(column_choice)
                        column_choice='"'+column_choice+'"'
                        cf=st.sidebar.button("Filterləri təmizlə")
                            
                        if  last_fifteen:
                            if full_script:
                                full_script += ' or '
                            start_date,end_date=funcc.getlast15date()
                            lst15=f'''{date_type} between  \'{start_date}\' and \'{end_date}\''''
                            full_script += lst15
                        else :
                            last_fifteen=False
                            lst15=False
                                    #st.write(start_date,end_date)
                        if  last_thirty:
                            if full_script:
                                full_script += ' or '
                            start_date,end_date=funcc.getlast30date()
                            lst30=f'''{date_type} between  \'{start_date}\' and \'{end_date}\''''
                            full_script += lst30
                        else: 
                            last_thirty=False
                            lst30=False
                        
                        if  cf == False :
                            cf=False
                            clrfltr=' '
                        else :
                            start_date,end_date=funcc.getdate()
                            #st.write(start_date,end_date)
                            full_script=f'''  {date_type} between  \'{start_date}\' and \'{end_date}\' ''' 
                            lst30=True  
                            lst15=True
                        if full_script:
                            scripts = f" ({full_script.strip()})"
                        else :
                            start_date,end_date=funcc.getdate()
                            #st.write(start_date,end_date)
                            scripts=f'''  {date_type} between  \'{start_date}\' and \'{end_date}\' '''
                        
                
                        
                        a4=st.sidebar.button("Yarat")
                        if a4:
                            
                            vv='Report '
                            st.header (vv)
                                
                                                                
                            SQl_query_filtered=mpay_payment_checks(conn,column_choice,scripts,mp_pr,mp_sr,mpay_point,status,mpay_ccount)
                            SQl_query_filtered = SQl_query_filtered.fillna('')
                            
                                                
                            
                                
                            a=pd.DataFrame(SQl_query_filtered)
                                
                                
                                       
                            numeric_cols=['sum_income','sum_outcome']
                                        
                            result = pd.DataFrame([{col: int(a[col].sum()) for col in numeric_cols}]).T
                                        
                            result = result.reset_index(drop=True).T
                            result.columns=['sum_income','sum_outcome']
                            result = result.reset_index()
                                        
                            result.columns=[' ','sum(sum_income)','sum(sum_outcome)']
                            result["count"]=pd.DataFrame(a.count()).iloc[0,0]
                            result.iloc[0,0]='Total'
                            #result = result.set_index('Total')
                            funcc.dataframee1(result)
                            funcc.dataframee(SQl_query_filtered)
                            #st.write(SQl_query_filtered)
                            
                        else:
                            st.markdown(
    """
    <style>
        div [data-testid=stImage]{
            text-align: center;
            display: block;
            margin-left: auto;
            margin-right: auto;
            width: 100%;
            margin-top: 20%;
        }

    </style>
    """, unsafe_allow_html=True
)
                            icon=Image.open('edatamart.png')        
                            st.image(icon)
                        dw=st.sidebar.checkbox("Yüklə")
                        if dw:
                            vv='Report '
                            SQl_query_filtered=mpay_payment_checks(conn,column_choice,scripts,mp_pr,mp_sr,mpay_point,status,mpay_ccount)
                            csv = funcc.convert_df(SQl_query_filtered)
                            st.sidebar.header("Yüklə:")
                            st.sidebar.download_button(label="CSV",data=csv,file_name=vv+'.csv',mime='text/csv'    )
                            st.sidebar.download_button(label="TEXT",data=csv,file_name=vv+'.txt',mime='text')
                                #excel=funcc.to_excel(SQl_query_filtered)
                            if len(SQl_query_filtered.index)>1000000:
                                cl=st.sidebar.checkbox("Excel")
                                if cl:
                                    excel=funcc.to_excell(SQl_query_filtered)
                                    st.sidebar.download_button(label="EXCEL",data=excel,file_name=vv+'.xlsx',mime='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
                            else:
                                excel=funcc.to_excel(SQl_query_filtered)
                                st.sidebar.download_button(label="EXCEL",data=excel,file_name=vv+'.xlsx',mime='xlsx')                            

                        
                                    
                    else :
                        servicenamemod=servicename[servicename['Service ID'].notnull()]  
                        
                         
                        if company=='Modenis' :
                            if modeniss_container:
                                prrnamemod=np.unique(servicenamemod["Provider yeni adlar"])
                            
                                prrasmod = st.sidebar.multiselect('Provider', prrnamemod,help='Bütün providerləri seçmək üçün heç bir seçim etməyin')
                                if prrasmod !=[]:
                                    prname=servicename[servicename["Provider yeni adlar"].isin(prrasmod)]
                                # st.write(prname)
                                    if len(prrasmod)==1:
                                        prrasmod     = tuple(prrasmod)
                                        prrasmod     = "('{}')".format(prrasmod    [0]) 
                                        mp_pr=f' and "Provider" in {prrasmod}'
                                        
                                    elif len(prrasmod)>1 :
                                        mp_pr=f' and  "Provider" in {tuple(prrasmod)}'
                                    
                                else :
                                    prname=servicename
                                    mp_pr=' '
                                sr_name=np.unique(prname["Service name yeni adlar"])
                                servmod = st.sidebar.multiselect('Xidmət',sr_name)
                                if servmod==[]:
                                    servmod=sr_name
                                    mp_sr=' '
                                elif len(servmod)==1:
                                    servmod     = tuple(servmod)
                                    servmod     = "('{}')".format(servmod    [0]) 
                                    mp_sr=f' and "Service" in {servmod} '
                                else:
                                    mp_sr=f' and "Service" in {tuple(servmod)} '
                                # st.write(mp_pr)
                                # st.write(mp_sr)
                                            
                                stt=('Success', 'Revoked','Processing' )
                                status=st.sidebar.multiselect("Status",stt,default="Success",help='Bütün statusları seçmək üçün heç bir seçim etməyin')
                                status=tuple(status)
                                if status ==():
                                    status=stt
                                    status=' '
                                        
                                elif len(status)>1:
                                    status = status
                                    status=f' and  status in {status} '
                                elif len(status)==1:
                                    status = tuple(status)
                                    status = "('{}')".format(status[0])
                                    status=f' and  status in {status} '
                                mod_chashout=st.sidebar.text_input('İnkassasiya id')
                                if mod_chashout!='':
                                    mod_ccount=f' and m.cashoutid in ({mod_chashout})'
                                else:
                                    mod_ccount=' '
                                new_row=pd.Series(['All'])
                                #pointidd_mod = point_mod.append(new_row, ignore_index=True)
                                pointidd_mod=pd.concat([point_mod,new_row])
                                pointmod=st.sidebar.multiselect("Pointid",pointidd_mod,default='All')
                                pointmod=tuple(pointmod) 
                                if len(pointmod)>1:
                                    pointmod = pointmod
                                else:
                                    pointmod = tuple(pointmod)
                                    pointmod = "('{}')".format(pointmod[0])               
                                if 'All' in pointmod:
                                    point_mod= ' '
                                    
                                else : 
                                    point_mod= f'and  pointid in {pointmod}  '
                                
                                
                                st.sidebar.header("Tarixi seçin:")
                                date_typee=st.sidebar.radio("",("Ödəniş tarixi","İnkassasiya tarixi"),horizontal=True)
                                if date_typee=="Ödəniş tarixi":
                                    date_type=timepar
                                else:
                                    date_type=' m.cashoutdate '
                                a,b=st.sidebar.columns(2)     
                                
                                v=datetime.today().replace(day=1)
                                div=st.sidebar.container()
                                full_script=''
                                start_date=a.date_input("Başlanğıc tarix",v)
                                end_date=b.date_input("Son tarix")
                                f=a.checkbox("Filter")
                                st.sidebar.write(' ')
                                st.sidebar.write(' ')
                                c,d=st.sidebar.columns(2)
                                s2020=c.checkbox("2020")
                                s2021=d.checkbox("2021")
                                s2022=c.checkbox("2022")
                                s2023=d.checkbox("2023")
                                a=('Yanvar','Fevral','Mart','Aprel','May','İyun','İyul','Avqust','Sentyabr','Oktyabr','Noyabr','Dekabr')
                                full_script = '' 
                                if f:
                                    start_date=start_date
                                    end_date=end_date
                                    ccc=f'''  {date_type} between '{start_date}' and '{end_date}' '''
                                    if full_script:
                                        full_script += ' or '
                                    full_script += ccc
                                if s2020:
                                    year=2020
                                    ss2020=st.sidebar.multiselect('2020',a)
                                if s2021:
                                    year=2021
                                    ss2021=st.sidebar.multiselect('2021 ',a)
                                if s2022:
                                    year=2022
                                    ss2022=st.sidebar.multiselect('2022 ',a)
                                if s2023:
                                    year=2023
                                    ss2023=st.sidebar.multiselect('2023 ',a)
                                if s2020:
                                    if ss2020!= ' ':
                                        year=2020
                                        #full_script = ''   
                                        if 'Yanvar' in ss2020:
                                            jan_script=f'''  {date_type} between \'{year}-01-01\' and \'{year}-02-01\' '''
                                            if full_script:
                                                full_script += ' or '
                                            full_script += jan_script
                                        else :
                                            jan_script=' '
                                        if 'Fevral' in ss2020:
                                            feb_script=f'''  {date_type} between \'{year}-02-01\' and \'{year}-03-01\' '''
                                            if full_script:
                                                full_script += ' or '
                                            full_script += feb_script
                                        else :
                                            feb_script=' '
                                        if 'Mart' in ss2020:
                                            if full_script:
                                                full_script += ' or '
                                            mar_script=f'''  {date_type} between \'{year}-03-01\' and \'{year}-04-01\' '''
                                            full_script += mar_script
                                        else :
                                            mar_script=' '
                                        if 'Aprel' in ss2020:
                                            if full_script:
                                                full_script += ' or '
                                            apr_script=f'''  {date_type} between \'{year}-04-01\' and \'{year}-05-01\' '''
                                            full_script += apr_script
                                        else :
                                            apr_script=' '
                                        if 'May' in ss2020:
                                            if full_script:
                                                full_script += ' or '
                                            may_script=f'''  {date_type} between \'{year}-05-01\' and \'{year}-06-01\' '''
                                            full_script += may_script
                                        else :
                                            may_script=' '
                                        if 'İyun' in ss2020:
                                            if full_script:
                                                full_script += ' or '
                                            jun_script=f'''  {date_type} between \'{year}-06-01\' and \'{year}-07-01\' '''
                                            full_script += jun_script
                                        else :
                                            jun_script=' '
                                        if 'İyul' in ss2020:
                                            if full_script:
                                                full_script += ' or '
                                            jul_script=f'''  {date_type} between \'{year}-07-01\' and \'{year}-08-01\' '''
                                            full_script += jul_script
                                        else :
                                            jul_script=' '
                                        if 'Avqust' in ss2020:
                                            if full_script:
                                                full_script += ' or '
                                            avg_script=f'''  {date_type} between \'{year}-08-01\' and \'{year}-09-01\' '''
                                            full_script += avg_script
                                        else :
                                            avg_script=' '
                                        if 'Sentyabr' in ss2020:
                                            if full_script:
                                                full_script += ' or '
                                            sen_script=f'''  {date_type} between \'{year}-09-01\' and \'{year}-10-01\' '''
                                            full_script += sen_script
                                        else :
                                            sen_script=' '
                                        if 'Oktyabr' in ss2020:
                                            if full_script:
                                                full_script += ' or '
                                            oct_script=f'''  {date_type} between \'{year}-10-01\' and \'{year}-11-01\' '''
                                            full_script += oct_script
                                        else :
                                            oct_script=' '
                                        if 'Noyabr' in ss2020:
                                            if full_script:
                                                full_script += ' or '
                                            nov_script=f'''  {date_type} between \'{year}-11-01\' and \'{year}-12-01\' '''
                                            full_script += nov_script
                                        else :
                                            nov_script=' '
                                        if 'Dekabr' in ss2020:
                                            if full_script:
                                                full_script += ' or '
                                            dec_script=f'''  {date_type} between \'{year}-12-01\' and \'{year+1}-01-01\' '''
                                            full_script += dec_script
                                        else :
                                            dec_script=' '
                                if s2021:
                                    if ss2021!= ' ':
                                        #full_script = ''  
                                        year=2021 
                                        if 'Yanvar' in ss2021:
                                            if full_script:
                                                full_script += ' or '
                                            jan_script=f'''  {date_type} between \'{year}-01-01\' and \'{year}-02-01\' '''
                                            full_script += jan_script
                                        else :
                                            jan_script=' '
                                        if 'Fevral' in ss2021:
                                            feb_script=f'''  {date_type} between \'{year}-02-01\' and \'{year}-03-01\' '''
                                            if full_script:
                                                full_script += ' or '
                                            full_script += feb_script
                                        else :
                                            feb_script=' '
                                        if 'Mart' in ss2021:
                                            if full_script:
                                                full_script += ' or '
                                            mar_script=f'''  {date_type} between \'{year}-03-01\' and \'{year}-04-01\' '''
                                            full_script += mar_script
                                        else :
                                            mar_script=' '
                                        if 'Aprel' in ss2021:
                                            if full_script:
                                                full_script += ' or '
                                            apr_script=f'''  {date_type} between \'{year}-04-01\' and \'{year}-05-01\' '''
                                            full_script += apr_script
                                        else :
                                            apr_script=' '
                                        if 'May' in ss2021:
                                            if full_script:
                                                full_script += ' or '
                                            may_script=f'''  {date_type} between \'{year}-05-01\' and \'{year}-06-01\' '''
                                            full_script += may_script
                                        else :
                                            may_script=' '
                                        if 'İyun' in ss2021:
                                            if full_script:
                                                full_script += ' or '
                                            jun_script=f'''  {date_type} between \'{year}-06-01\' and \'{year}-07-01\' '''
                                            full_script += jun_script
                                        else :
                                            jun_script=' '
                                        if 'İyul' in ss2021:
                                            if full_script:
                                                full_script += ' or '
                                            jul_script=f'''  {date_type} between \'{year}-07-01\' and \'{year}-08-01\' '''
                                            full_script += jul_script
                                        else :
                                            jul_script=' '
                                        if 'Avqust' in ss2021:
                                            if full_script:
                                                full_script += ' or '
                                            avg_script=f'''  {date_type} between \'{year}-08-01\' and \'{year}-09-01\' '''
                                            full_script += avg_script
                                        else :
                                            avg_script=' '
                                        if 'Sentyabr' in ss2021:
                                            if full_script:
                                                full_script += ' or '
                                            sen_script=f'''  {date_type} between \'{year}-09-01\' and \'{year}-10-01\' '''
                                            full_script += sen_script
                                        else :
                                            sen_script=' '
                                        if 'Oktyabr' in ss2021:
                                            if full_script:
                                                full_script += ' or '
                                            oct_script=f'''  {date_type} between \'{year}-10-01\' and \'{year}-11-01\' '''
                                            full_script += oct_script
                                        else :
                                            oct_script=' '
                                        if 'Noyabr' in ss2021:
                                            if full_script:
                                                full_script += ' or '
                                            nov_script=f'''  {date_type} between \'{year}-11-01\' and \'{year}-12-01\' '''
                                            full_script += nov_script
                                        else :
                                            nov_script=' '
                                        if 'Dekabr' in ss2021:
                                            if full_script:
                                                full_script += ' or '
                                            dec_script=f'''  {date_type} between \'{year}-12-01\' and \'{year+1}-01-01\' '''
                                            full_script += dec_script
                                        else :
                                            dec_script=' '
                                if s2022:
                                    if ss2022!= ' ':
                                        #full_script = ''   
                                        year=2022
                                        if 'Yanvar' in ss2022:
                                            if full_script:
                                                full_script += ' or '
                                            jan_script=f'''  {date_type} between \'{year}-01-01\' and \'{year}-02-01\' '''
                                            full_script += jan_script
                                        else :
                                            jan_script=' '
                                        if 'Fevral' in ss2022:
                                            feb_script=f'''  {date_type} between \'{year}-02-01\' and \'{year}-03-01\' '''
                                            if full_script:
                                                full_script += ' or '
                                            full_script += feb_script
                                        else :
                                            feb_script=' '
                                        if 'Mart' in ss2022:
                                            if full_script:
                                                full_script += ' or '
                                            mar_script=f'''  {date_type} between \'{year}-03-01\' and \'{year}-04-01\' '''
                                            full_script += mar_script
                                        else :
                                            mar_script=' '
                                        if 'Aprel' in ss2022:
                                            if full_script:
                                                full_script += ' or '
                                            apr_script=f'''  {date_type} between \'{year}-04-01\' and \'{year}-05-01\' '''
                                            full_script += apr_script
                                        else :
                                            apr_script=' '
                                        if 'May' in ss2022:
                                            if full_script:
                                                full_script += ' or '
                                            may_script=f'''  {date_type} between \'{year}-05-01\' and \'{year}-06-01\' '''
                                            full_script += may_script
                                        else :
                                            may_script=' '
                                        if 'İyun' in ss2022:
                                            if full_script:
                                                full_script += ' or '
                                            jun_script=f'''  {date_type} between \'{year}-06-01\' and \'{year}-07-01\' '''
                                            full_script += jun_script
                                        else :
                                            jun_script=' '
                                        if 'İyul' in ss2022:
                                            if full_script:
                                                full_script += ' or '
                                            jul_script=f'''  {date_type} between \'{year}-07-01\' and \'{year}-08-01\' '''
                                            full_script += jul_script
                                        else :
                                            jul_script=' '
                                        if 'Avqust' in ss2022:
                                            if full_script:
                                                full_script += ' or '
                                            avg_script=f'''  {date_type} between \'{year}-08-01\' and \'{year}-09-01\' '''
                                            full_script += avg_script
                                        else :
                                            avg_script=' '
                                        if 'Sentyabr' in ss2022:
                                            if full_script:
                                                full_script += ' or '
                                            sen_script=f'''  {date_type} between \'{year}-09-01\' and \'{year}-10-01\' '''
                                            full_script += sen_script
                                        else :
                                            sen_script=' '
                                        if 'Oktyabr' in ss2022:
                                            if full_script:
                                                full_script += ' or '
                                            oct_script=f'''  {date_type} between \'{year}-10-01\' and \'{year}-11-01\' '''
                                            full_script += oct_script
                                        else :
                                            oct_script=' '
                                        if 'Noyabr' in ss2022:
                                            if full_script:
                                                full_script += ' or '
                                            nov_script=f'''  {date_type} between \'{year}-11-01\' and \'{year}-12-01\' '''
                                            full_script += nov_script
                                        else :
                                            nov_script=' '
                                        if 'Dekabr' in ss2022:
                                            if full_script:
                                                full_script += ' or '
                                            dec_script=f'''  {date_type} between \'{year}-12-01\' and \'{year+1}-01-01\' '''
                                            full_script += dec_script
                                        else :
                                            dec_script=' '
                                if s2023:
                                    if ss2023!= ' ':
                                        #full_script = ''   
                                        year=2023
                                        if 'Yanvar' in ss2023:
                                            if full_script:
                                                full_script += ' or '
                                            jan_script=f'''  {date_type} between \'{year}-01-01\' and \'{year}-02-01\' '''
                                            full_script += jan_script
                                        else :
                                            jan_script=' '
                                        if 'Fevral' in ss2023:
                                            feb_script=f'''  {date_type} between \'{year}-02-01\' and \'{year}-03-01\' '''
                                            if full_script:
                                                full_script += ' or '
                                            full_script += feb_script
                                        else :
                                            feb_script=' '
                                        if 'Mart' in ss2023:
                                            if full_script:
                                                full_script += ' or '
                                            mar_script=f'''  {date_type} between \'{year}-03-01\' and \'{year}-04-01\' '''
                                            full_script += mar_script
                                        else :
                                            mar_script=' '
                                        if 'Aprel' in ss2023:
                                            if full_script:
                                                full_script += ' or '
                                            apr_script=f'''  {date_type} between \'{year}-04-01\' and \'{year}-05-01\' '''
                                            full_script += apr_script
                                        else :
                                            apr_script=' '
                                        if 'May' in ss2023:
                                            if full_script:
                                                full_script += ' or '
                                            may_script=f'''  {date_type} between \'{year}-05-01\' and \'{year}-06-01\' '''
                                            full_script += may_script
                                        else :
                                            may_script=' '
                                        if 'İyun' in ss2023:
                                            if full_script:
                                                full_script += ' or '
                                            jun_script=f'''  {date_type} between \'{year}-06-01\' and \'{year}-07-01\' '''
                                            full_script += jun_script
                                        else :
                                            jun_script=' '
                                        if 'İyul' in ss2023:
                                            if full_script:
                                                full_script += ' or '
                                            jul_script=f'''  {date_type} between \'{year}-07-01\' and \'{year}-08-01\' '''
                                            full_script += jul_script
                                        else :
                                            jul_script=' '
                                        if 'Avqust' in ss2023:
                                            if full_script:
                                                full_script += ' or '
                                            avg_script=f'''  {date_type} between \'{year}-08-01\' and \'{year}-09-01\' '''
                                            full_script += avg_script
                                        else :
                                            avg_script=' '
                                        if 'Sentyabr' in ss2023:
                                            if full_script:
                                                full_script += ' or '
                                            sen_script=f'''  {date_type} between \'{year}-09-01\' and \'{year}-10-01\' '''
                                            full_script += sen_script
                                        else :
                                            sen_script=' '
                                        if 'Oktyabr' in ss2023:
                                            if full_script:
                                                full_script += ' or '
                                            oct_script=f'''  {date_type} between \'{year}-10-01\' and \'{year}-11-01\' '''
                                            full_script += oct_script
                                        else :
                                            oct_script=' '
                                        if 'Noyabr' in ss2023:
                                            if full_script:
                                                full_script += ' or '
                                            nov_script=f'''  {date_type} between \'{year}-11-01\' and \'{year}-12-01\' '''
                                            full_script += nov_script
                                        else :
                                            nov_script=' '
                                        if 'Dekabr' in ss2023:
                                            if full_script:
                                                full_script += ' or '
                                            dec_script=f'''  {date_type} between \'{year}-12-01\' and \'{year+1}-01-01\' '''
                                            full_script += dec_script
                                        else :
                                            dec_script=' '
                                

                                
                                ss,dd=st.sidebar.columns(2)
                                last_fifteen=ss.checkbox("Son 15 gün")
                                last_thirty=dd.checkbox("Son 30 gün")
                                cf=st.sidebar.button("Filterləri təmizlə")
                                    
                                if  last_fifteen:
                                    if full_script:
                                        full_script += ' or '
                                    start_date,end_date=funcc.getlast15date()
                                    lst15=f'''{date_type} between  \'{start_date}\' and \'{end_date}\''''
                                    full_script += lst15
                                else :
                                    last_fifteen=False
                                    lst15=False
                                            #st.write(start_date,end_date)
                                if  last_thirty:
                                    if full_script:
                                        full_script += ' or '
                                    start_date,end_date=funcc.getlast30date()
                                    lst30=f'''{date_type} between  \'{start_date}\' and \'{end_date}\''''
                                    full_script += lst30
                                else: 
                                    last_thirty=False
                                    lst30=False
                                
                                if  cf == False :
                                    cf=False
                                    clrfltr=' '
                                else :
                                    start_date,end_date=funcc.getdate()
                                    #st.write(start_date,end_date)
                                    full_script=f'''  {date_type} between  \'{start_date}\' and \'{end_date}\' ''' 
                                    lst30=True  
                                    lst15=True
                                if full_script:
                                    scripts = f" ({full_script.strip()})"
                                else :
                                    start_date,end_date=funcc.getdate()
                                    #st.write(start_date,end_date)
                                    scripts=f'''  {date_type} between  \'{start_date}\' and \'{end_date}\' '''
                                
                                                
                                                
                                #st.write(servi_mod)
                                b2=st.sidebar.button("Yarat")
                                if b2:
                                    
                                    vv='Report '
                                    st.header (vv)
                                    
                                    SQl_query_filtered=modenis_paymentcheck(conn,mp_sr,point_mod,scripts,status,mp_pr,mod_ccount)
                                    SQl_query_filtered = SQl_query_filtered.fillna('')
                                    len_data=len(SQl_query_filtered.index)
                                    a=pd.DataFrame(SQl_query_filtered)
                                       
                                    numeric_cols=['payvalue','servicevalue']
                                    
                                    result = pd.DataFrame([{col: int(a[col].sum()) for col in numeric_cols}]).T
                                        
                                    result = result.reset_index(drop=True).T
                                    result.columns=['payvalue','servicevalue']
                                    result = result.reset_index()
                                        
                                    result.columns=[' ','sum(payvalue)','sum(servicevalue)']
                                    result["count"]=pd.DataFrame(a.count()).iloc[0,0]
                                    result.iloc[0,0]='Total'
                                    #result = result.set_index('Total')
                                    
                                    
                                    funcc.dataframee1(result)
                                    

                                    #ex=st.sidebar.checkbox('Download as excel')
                                    funcc.dataframee(SQl_query_filtered)
                                    
                                else:
                                    st.markdown(
    """
    <style>
        div [data-testid=stImage]{
            text-align: center;
            display: block;
            margin-left: auto;
            margin-right: auto;
            width: 100%;
            margin-top: 20%;
        }

    </style>
    """, unsafe_allow_html=True
)
                                    icon=Image.open('edatamart.png')        
                                    st.image(icon)
                                dw=st.sidebar.checkbox("Yüklə")
                                if dw:
                                    vv='Report '
                                    SQl_query_filtered=modenis_paymentcheck(conn,mp_sr,point_mod,scripts,status,mp_pr,mod_ccount)
                                    csv = funcc.convert_df(SQl_query_filtered)
                                    st.sidebar.header("Yüklə:")
                                    st.sidebar.download_button(label="CSV",data=csv,file_name=vv+'.csv',mime='text/csv'    )
                                    st.sidebar.download_button(label="TEXT",data=csv,file_name=vv+'.txt',mime='text')
                                        #excel=funcc.to_excel(SQl_query_filtered)
                                    if len(SQl_query_filtered.index)>1000000:
                                        cl=st.sidebar.checkbox("Excel")
                                        if cl:
                                            excel=funcc.to_excell(SQl_query_filtered)
                                            st.sidebar.download_button(label="EXCEL",data=excel,file_name=vv+'.xlsx',mime='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
                                    else:
                                        excel=funcc.to_excel(SQl_query_filtered)
                                        st.sidebar.download_button(label="EXCEL",data=excel,file_name=vv+'.xlsx',mime='xlsx')     
                if  year_choices==True:
                    modeniss_container =st.container() 
                    modeniss_g_container =st.container()
                    mpayy_container =st.container() 
                    mpayy_g_container =st.container()
                    pr,sr,pointid_mp,pr_mod,ser_mod,point_mod,servicename=getdata(conn)   
                    provider_name = pr["providername"]
                    service_name_mpay=sr["servicename"]
                    
                    prid_mpay=pr["id_provider"] 
                    provider_name_mod = pr_mod["nomenclaturename"]
                    if company=='All':
                        pr,sr,pointid_mp,pr_mod,ser_mod,point_mod,servicename=getdata(conn) 
                        sdd,point=dfg(conn)
                
                        prrname=np.unique(sdd["Provider yeni adlar"])
                    
                        #prrname=prrname.sort()
                        #st.write(prrname)
                        prras = st.sidebar.multiselect('Provider',prrname)
                        if prras !=[]:
                            prname=sdd[sdd["Provider yeni adlar"].isin(prras)]
                            #st.write(prname)
                            if len(prras)==1:
                                prras     = tuple(prras)
                                prras     = "('{}')".format(prras    [0]) 
                                mp_pr=f''' and  replace ( replace ( "Provider yeni adlar",'[MP] ','' ),'[MOD] ','' ) in {prras}'''
                                
                            elif len(prras)>1 :
                                mp_pr=f''' and  replace ( replace ( "Provider yeni adlar",'[MP] ','' ),'[MOD] ','' )in {tuple(prras)}'''
                            
                        else :
                            prname=servicename
                            mp_pr=' '
                        sr_name=np.unique(prname["Service name yeni adlar"])
                        serv = st.sidebar.multiselect('Xidmət',sr_name)
                        if serv==[]:
                            serv=sr_name
                            mp_sr=' '
                        elif len(serv)==1:
                            serv     = tuple(serv)
                            serv     = "('{}')".format(serv    [0]) 
                            mp_sr=f' and "Service" in {serv} '
                        else:
                            mp_sr=f' and "Service" in {tuple(serv)} '
                        #point=np.unique(point["pointid"])
                        
                        #st.write(point)
                        
                        
                        #pointidd_mpay = pointid_mpay.append(new_row, ignore_index=True)
                        stt=('Success', 'Revoked','Processing', 'Success/Cancelled ','Success/Funds return ', 'Success/Corrected ', 'Success/Blocked ', 'Error/Cancelled by user', 'Error/Cancelled by support', 'Error/Rejected by provider', 'Error/Corrected', 'Error/Uncorrectable error', 'Error/Revocated', 'Error/Blocked by user', 'Processing/Provider determined', 'Processing/Processing', 'Processing/Unknown', 'Processing/Picked up', 'Processing/Error', 'Processing/In wait','Processing/Confirm' )
                        status=st.sidebar.multiselect("Status",stt,default="Success",help='Bütün statusları seçmək üçün heç bir seçim etməyin')
                        status=tuple(status)
                        if status ==():
                            status=stt
                            status=' '
                                                
                        elif len(status)>1:
                            status = status
                            status=f'   status in {status} '
                        elif len(status)==1:
                            status = tuple(status)
                            status = "('{}')".format(status[0])
                            status=f'   status in {status}'
                        
                        timepar=' time_server '
                        ttm=' createtime '
                        st.sidebar.header("Tarixi seçin:")
                        date_type=st.sidebar.radio("",("Ödəniş tarixi","İnkassasiya tarixi"),horizontal=True)
                        if date_type=="Ödəniş tarixi":
                            date_type=timepar
                        else:
                            date_type=collection_time
                        
                        
                        c,d=st.sidebar.columns(2)
                        s2020=c.checkbox("2020")
                        s2021=d.checkbox("2021")
                        s2022=c.checkbox("2022")
                        s2023=d.checkbox("2023")
                        if s2020:
                            start_date,end_date=funcc.twenty()
                            a20=f'''{timepar} between  \'{start_date}\' and \'{end_date}\''''
                        else :
                            a20=False
                        if s2021:
                            start_date,end_date=funcc.twentyone()
                            a21=f'''{timepar} between  \'{start_date}\' and \'{end_date}\''''
                        else :
                            a21=False
                        if s2022:
                            start_date,end_date=funcc.twentytwo()
                            a22=f'''{timepar} between  \'{start_date}\' and \'{end_date}\''''
                        else :
                            a22=False
                        if s2023:
                            start_date,end_date=funcc.twentythree()
                            a23=f'''{timepar} between  \'{start_date}\' and \'{end_date}\''''
                        else :
                            a23=False
                        if s2020==False and s2021==False  and s2022==False and s2023==False :
                            dd=f'''  {timepar} between \'{current_year}-01-01\' and \'{next_year}-01-01\''''
                        else : 
                            dd=False
                            
                        # sdas=scripts.replace('time_server','createtime')
                    
                        # pointmod=pointmpay.replace('id_point','pointid')
                        if dd!=False:
                            ddd=dd.replace('time_server','createtime')
                        else : ddd=False
                        if a20!=False:
                            a200=a20.replace('time_server','createtime')    
                        else : a200=False
                        if a21!=False:
                            a211=a21.replace('time_server','createtime')
                        else : a211=False
                        if a22!=False:
                            a222=a22.replace('time_server','createtime')
                        else : a222=False
                        if a23!=False:
                            a233=a23.replace('time_server','createtime')
                        else : a233=False
                        a4=st.sidebar.button("Yarat")
                        if a4:
                                    
                            vv='Report '
                            st.header (vv)
                                        
                                                                    
                            SQl_query_filtered=all_transactions_group(conn,dd,a20,a21,a22,a23,ddd,a200,a211,a222,a233,mp_pr,mp_sr,status)
                            SQl_query_filtered = SQl_query_filtered.fillna('')
                                    
                                                        
                                    
                                        
                            a=pd.DataFrame(SQl_query_filtered)
                            numeric_cols=['sum_income','sum_outcome']
                                                
                            result = pd.DataFrame([{col: int(a[col].sum()) for col in numeric_cols}]).T
                                                
                            result = result.reset_index(drop=True).T
                            result.columns=['sum_income','sum_outcome']
                            result = result.reset_index()
                                                
                            result.columns=[' ','sum(sum_income)','sum(sum_outcome)']
                            result["count"]=pd.DataFrame(a.count()).iloc[0,0]
                            result.iloc[0,0]='Total'
                                    #result = result.set_index('Total')
                            funcc.dataframee1(result)
                            funcc.dataframee(SQl_query_filtered)
                            

                        
                            
                        
                            
                            
                            
                     
                    elif company=='Mpay':
                        
                        prras = st.sidebar.multiselect('Provider', prrname,help='Bütün providerləri seçmək üçün heç bir seçim etməyin')
                        if prras !=[]:
                            prname=servicename[servicename["Provider yeni adlar"].isin(prras)]
                            
                            if len(prras)==1:
                                prras     = tuple(prras)
                                prras     = "('{}')".format(prras    [0]) 
                                mp_pr=f' and "Provider yeni adlar" in {prras}'
                                
                            elif len(prras)>1 :
                                mp_pr=f' and "Provider yeni adlar" in {tuple(prras)}'
                            
                        else :
                            prname=servicename
                            mp_pr=' '
                        sr_name=np.unique(prname["Service name yeni adlar"])
                        serv = st.sidebar.multiselect('Xidmət',sr_name)
                        if serv==[]:
                            serv=sr_name
                            mp_sr=' '
                        elif len(serv)==1:
                            serv     = tuple(serv)
                            serv     = "('{}')".format(serv    [0]) 
                            mp_sr=f' and "Service name yeni adlar" in {serv} '
                        else:
                            mp_sr=f' and "Service name yeni adlar" in {tuple(serv)} '
                                
                        stt=('Success', 'Success/Cancelled ','Success/Funds return ', 'Success/Corrected ', 'Success/Blocked ', 'Error/Cancelled by user', 'Error/Cancelled by support', 'Error/Rejected by provider', 'Error/Corrected', 'Error/Uncorrectable error', 'Error/Revocated', 'Error/Blocked by user', 'Processing/Provider determined', 'Processing/Processing', 'Processing/Unknown', 'Processing/Picked up', 'Processing/Error', 'Processing/In wait','Processing/Confirm' )
                        status=st.sidebar.multiselect("Status",stt,default="Success",help='Bütün statusları seçmək üçün heç bir seçim etməyin')
                        status=tuple(status)
                    
                        
                        if status ==():
                            status=stt
                            status=' '
                                #st.write(status)
                                #new_row = {'id_point': 'All'}
                        elif len(status)>1:
                            status = status
                            status=f' where status in {status} '
                        elif len(status)==1:
                            status = tuple(status)
                            status = "('{}')".format(status[0])
                            status=f' where status in {status} '
                                
                        sd,ds=st.sidebar.columns(2)
                        twenty1=sd.checkbox("2020")
                        twentytone1=ds.checkbox("2021")
                        twentytwo1=sd.checkbox("2022")
                        twentythree1=ds.checkbox("2023")
                        if twenty1:
                            start_date,end_date=funcc.twenty()
                            a20=f'''{timepar} between  \'{start_date}\' and \'{end_date}\''''
                        else :
                            a20=False
                        if twentytone1:
                            start_date,end_date=funcc.twentyone()
                            a21=f'''{timepar} between  \'{start_date}\' and \'{end_date}\''''
                        else :
                            a21=False
                        if twentytwo1:
                            start_date,end_date=funcc.twentytwo()
                            a22=f'''{timepar} between  \'{start_date}\' and \'{end_date}\''''
                        else :
                            a22=False
                        if twentythree1:
                            start_date,end_date=funcc.twentythree()
                            a23=f'''{timepar} between  \'{start_date}\' and \'{end_date}\''''
                        else :
                            a23=False
                        if twenty1==False and twentytone1==False  and twentytwo1==False and twentythree1==False :
                            dd=f'''  {timepar} between \'{current_year}-01-01\' and \'{next_year}-01-01\''''
                        else : 
                            dd=False
                            
                        a4=st.sidebar.button("Yarat")
                        if a4:
                           
                            vv='Report '
                            st.header (vv)
                            
                                                                
                            SQl_query_filtered=mpay_payment_check_group(conn,dd,a20,a21,a22,a23,mp_pr,mp_sr,status)
                            SQl_query_filtered = SQl_query_filtered.fillna('')
                            numeric_cols=['sum_income','count']
                            a=pd.DataFrame(SQl_query_filtered)            
                            result = pd.DataFrame([{col: int(a[col].sum()) for col in numeric_cols}]).T
                                        
                            result = result.reset_index(drop=True).T
                            result.columns=['sum_income','count']
                            result = result.reset_index()
                                        
                            result.columns=[' ','sum(sum_income)','count']
                            #result["count"]=pd.DataFrame(a.count()).iloc[0,0]
                            result.iloc[0,0]='Total'
                            #result = result.set_index('Total')
                            funcc.dataframee1(result)
                            funcc.dataframee(SQl_query_filtered)
                            
                            csv = funcc.convert_df(SQl_query_filtered)
                            st.sidebar.header("Yüklə:")
                            st.sidebar.download_button(label="CSV",data=csv,file_name=vv+'.csv',mime='text/csv'    )
                            st.sidebar.download_button(label="TEXT",data=csv,file_name=vv+'.txt',mime='text')
                            #excel=funcc.to_excel(SQl_query_filtered)
                            if len(SQl_query_filtered.index)>1000000:
                                cl=st.sidebar.checkbox("Excel")
                                if cl:
                                    excel=funcc.to_excell(SQl_query_filtered)
                                    st.sidebar.download_button(label="EXCEL",data=excel,file_name=vv+'.xlsx',mime='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
                            else:
                                excel=funcc.to_excel(SQl_query_filtered)
                                st.sidebar.download_button(label="EXCEL",data=excel,file_name=vv+'.xlsx',mime='xlsx')   
                        else:
                            st.markdown(
    """
    <style>
        div [data-testid=stImage]{
            text-align: center;
            display: block;
            margin-left: auto;
            margin-right: auto;
            width: 100%;
            margin-top: 20%;
        }

    </style>
    """, unsafe_allow_html=True
)
                            icon=Image.open('edatamart.png')        
                            st.image(icon)
                                    
                    else :
                       
                        servicenamemod=servicename[servicename['Service ID'].notnull()]    
                        if company=='Modenis' :
                            if modeniss_container:
                                prrnamemod=np.unique(servicenamemod["Provider yeni adlar"])
                            
                                prrasmod = st.sidebar.multiselect('Provider', prrnamemod,help='Bütün providerləri seçmək üçün heç bir seçim etməyin')
                                if prrasmod !=[]:
                                    prname=servicename[servicename["Provider yeni adlar"].isin(prrasmod)]
                                # st.write(prname)
                                    if len(prrasmod)==1:
                                        prrasmod     = tuple(prrasmod)
                                        prrasmod     = "('{}')".format(prrasmod    [0]) 
                                        mp_pr=f' and "Provider yeni adlar" in {prrasmod}'
                                        
                                    elif len(prrasmod)>1 :
                                        mp_pr=f' and  "Provider yeni adlar" in {tuple(prrasmod)}'
                                    
                                else :
                                    prname=servicename
                                    mp_pr=' '
                                sr_name=np.unique(prname["Service name yeni adlar"])
                                servmod = st.sidebar.multiselect('Xidmət',sr_name)
                                if servmod==[]:
                                    servmod=sr_name
                                    mp_sr=' '
                                elif len(servmod)==1:
                                    servmod     = tuple(servmod)
                                    servmod     = "('{}')".format(servmod    [0]) 
                                    mp_sr=f' and "Service name yeni adlar" in {servmod} '
                                else:
                                    mp_sr=f' and"Service name yeni adlar" in {tuple(servmod)} '
                                # st.write(mp_pr)
                                # st.write(mp_sr)
                                            
                                stt=('Success', 'Revoked','Processing' )
                                status=st.sidebar.multiselect("Status",stt,default="Success",help='Bütün statusları seçmək üçün heç bir seçim etməyin')
                                status=tuple(status)
                                if status ==():
                                    status=stt
                                    status=' '
                                        
                                elif len(status)>1:
                                    status = status
                                   # status=f' and  status in {status} '
                                elif len(status)==1:
                                    status = tuple(status)
                                    status = "('{}')".format(status[0])
                                    #status=f' and  status in {status} '
                                new_row=pd.Series(['All'])
                                
                                
   
                                
                                
                                    
                                sd,ds=st.sidebar.columns(2)
                                twenty1=sd.checkbox("2020")
                                twentytone1=ds.checkbox("2021")
                                twentytwo1=sd.checkbox("2022")
                                twentythree1=ds.checkbox("2023")
                                if twenty1:
                                    start_date,end_date=funcc.twenty()
                                    a20=f'''{timepar} between  \'{start_date}\' and \'{end_date}\''''
                                else :
                                    a20=False
                                if twentytone1:
                                    start_date,end_date=funcc.twentyone()
                                    a21=f'''{timepar} between  \'{start_date}\' and \'{end_date}\''''
                                else :
                                    a21=False
                                if twentytwo1:
                                    start_date,end_date=funcc.twentytwo()
                                    a22=f'''{timepar} between  \'{start_date}\' and \'{end_date}\''''
                                else :
                                    a22=False
                                if twentythree1:
                                    start_date,end_date=funcc.twentythree()
                                    a23=f'''{timepar} between  \'{start_date}\' and \'{end_date}\''''
                                else :
                                    a23=False
                                if twenty1==False and twentytone1==False  and twentytwo1==False and twentythree1==False :
                                    dd=f'''  {timepar} between \'{current_year}-01-01\' and \'{next_year}-01-01\''''
                                else : 
                                    dd=False
                                #st.write(servi_mod)
                                b2=st.sidebar.button("Yarat")
                                if b2:
                                    
                                    vv='Report '
                                    st.header (vv)
                                        
                                    SQl_query_filtered=modenis_group_payment_check(conn,dd,a20,a21,a22,a23,mp_sr,mp_pr,status)
                                    SQl_query_filtered = SQl_query_filtered.fillna('')
                                    len_data=len(SQl_query_filtered.index)
                                    a=pd.DataFrame(SQl_query_filtered)
                                       
                                    numeric_cols=['payvalue','count']
                                    
                                    result = pd.DataFrame([{col: int(a[col].sum()) for col in numeric_cols}]).T
                                        
                                    result = result.reset_index(drop=True).T
                                    result.columns=['payvalue','count']
                                    result = result.reset_index()
                                        
                                    result.columns=[' ','sum(payvalue)','count']
                                    #result["count"]=pd.DataFrame(a.count()).iloc[0,0]
                                    result.iloc[0,0]='Total'
                                    #result = result.set_index('Total')
                                    
                                    
                                    funcc.dataframee1(result)
                                    funcc.dataframee(SQl_query_filtered)
                                    csv = funcc.convert_df(SQl_query_filtered)
                                    st.sidebar.header("Yüklə:")
                                    st.sidebar.download_button(label="CSV",data=csv,file_name=vv+'.csv',mime='text/csv'    )
                                    st.sidebar.download_button(label="TEXT",data=csv,file_name=vv+'.txt',mime='text')
                                    #excel=funcc.to_excel(SQl_query_filtered)
                                    if len_data>1000000:
                                        excel=funcc.to_excell(SQl_query_filtered)
                                        ex=st.sidebar.checkbox('Excel')
                                        if ex:
                                            st.sidebar.download_button(label="EXCEL",data=excel,file_name=vv+'.xlsx',mime='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
                                    else:
                                        excel=funcc.to_excel(SQl_query_filtered)
                                        st.sidebar.download_button(label="EXCEL",data=excel,file_name=vv+'.xlsx',mime='xlsx')
                                else :
                                    st.markdown(
    """
    <style>
        div [data-testid=stImage]{
            text-align: center;
            display: block;
            margin-left: auto;
            margin-right: auto;
            width: 100%;
            margin-top: 20%;
        }

    </style>
    """, unsafe_allow_html=True
        )
                                    icon=Image.open('edatamart.png')        
                                    st.image(icon)
                                                
            elif selected3=='Hazır hesabatlar':
                
             if st.container:
                selected2= option_menu("", ["ABB Korporativ hesab","Mpay terminal","Modenis terminal"])
                st.sidebar.header("Tarixi seçin:")
                g=st.sidebar.checkbox('İl')
                if selected2=='ABB Korporativ hesab': timepar=' "Date" '
                elif selected2=="Mpay terminal" :timepar=' time_server '
                else : timepar=' createtime ' 
                if g is False:
                    
                    a,b=st.sidebar.columns(2)     
                        
                    v=datetime.today().replace(day=1)
                    div=st.sidebar.container()
                    full_script=''
                    start_date=a.date_input("Başlanğıc tarix",v)
                    end_date=b.date_input("Son tarix")
                    f=a.checkbox("Filter")
                    st.sidebar.write(' ')
                    st.sidebar.write(' ')
                    c,d=st.sidebar.columns(2)
                    s2020=c.checkbox("2020")
                    s2021=d.checkbox("2021")
                    s2022=c.checkbox("2022")
                    s2023=d.checkbox("2023")
                    a=('Yanvar','Fevral','Mart','Aprel','May','İyun','İyul','Avqust','Sentyabr','Oktyabr','Noyabr','Dekabr')
                    full_script = '' 
                    if f:
                        start_date=start_date
                        end_date=end_date
                        ccc=f'''  {timepar} between '{start_date}' and '{end_date}' '''
                        if full_script:
                            full_script += ' or '
                        full_script += ccc
                    if s2020:
                        year=2020
                        ss2020=st.sidebar.multiselect('2020',a)
                    if s2021:
                        year=2021
                        ss2021=st.sidebar.multiselect('2021 ',a)
                    if s2022:
                        year=2022
                        ss2022=st.sidebar.multiselect('2022 ',a)
                    if s2023:
                        year=2023
                        ss2023=st.sidebar.multiselect('2023 ',a)
                    if s2020:
                        if ss2020!= ' ':
                            year=2020
                            #full_script = ''   
                            if 'Yanvar' in ss2020:
                                jan_script=f'''  {timepar} between \'{year}-01-01\' and \'{year}-02-01\' '''
                                if full_script:
                                    full_script += ' or '
                                full_script += jan_script
                            else :
                                jan_script=' '
                            if 'Fevral' in ss2020:
                                feb_script=f'''  {timepar} between \'{year}-02-01\' and \'{year}-03-01\' '''
                                if full_script:
                                    full_script += ' or '
                                full_script += feb_script
                            else :
                                feb_script=' '
                            if 'Mart' in ss2020:
                                if full_script:
                                    full_script += ' or '
                                mar_script=f'''  {timepar} between \'{year}-03-01\' and \'{year}-04-01\' '''
                                full_script += mar_script
                            else :
                                mar_script=' '
                            if 'Aprel' in ss2020:
                                if full_script:
                                    full_script += ' or '
                                apr_script=f'''  {timepar} between \'{year}-04-01\' and \'{year}-05-01\' '''
                                full_script += apr_script
                            else :
                                apr_script=' '
                            if 'May' in ss2020:
                                if full_script:
                                    full_script += ' or '
                                may_script=f'''  {timepar} between \'{year}-05-01\' and \'{year}-06-01\' '''
                                full_script += may_script
                            else :
                                may_script=' '
                            if 'İyun' in ss2020:
                                if full_script:
                                    full_script += ' or '
                                jun_script=f'''  {timepar} between \'{year}-06-01\' and \'{year}-07-01\' '''
                                full_script += jun_script
                            else :
                                jun_script=' '
                            if 'İyul' in ss2020:
                                if full_script:
                                    full_script += ' or '
                                jul_script=f'''  {timepar} between \'{year}-07-01\' and \'{year}-08-01\' '''
                                full_script += jul_script
                            else :
                                jul_script=' '
                            if 'Avqust' in ss2020:
                                if full_script:
                                    full_script += ' or '
                                avg_script=f'''  {timepar} between \'{year}-08-01\' and \'{year}-09-01\' '''
                                full_script += avg_script
                            else :
                                avg_script=' '
                            if 'Sentyabr' in ss2020:
                                if full_script:
                                    full_script += ' or '
                                sen_script=f'''  {timepar} between \'{year}-09-01\' and \'{year}-10-01\' '''
                                full_script += sen_script
                            else :
                                sen_script=' '
                            if 'Oktyabr' in ss2020:
                                if full_script:
                                    full_script += ' or '
                                oct_script=f'''  {timepar} between \'{year}-10-01\' and \'{year}-11-01\' '''
                                full_script += oct_script
                            else :
                                oct_script=' '
                            if 'Noyabr' in ss2020:
                                if full_script:
                                    full_script += ' or '
                                nov_script=f'''  {timepar} between \'{year}-11-01\' and \'{year}-12-01\' '''
                                full_script += nov_script
                            else :
                                nov_script=' '
                            if 'Dekabr' in ss2020:
                                if full_script:
                                    full_script += ' or '
                                dec_script=f'''  {timepar} between \'{year}-12-01\' and \'{year+1}-01-01\' '''
                                full_script += dec_script
                            else :
                                dec_script=' '
                    if s2021:
                        if ss2021!= ' ':
                            #full_script = ''  
                            year=2021 
                            if 'Yanvar' in ss2021:
                                if full_script:
                                    full_script += ' or '
                                jan_script=f'''  {timepar} between \'{year}-01-01\' and \'{year}-02-01\' '''
                                full_script += jan_script
                            else :
                                jan_script=' '
                            if 'Fevral' in ss2021:
                                feb_script=f'''  {timepar} between \'{year}-02-01\' and \'{year}-03-01\' '''
                                if full_script:
                                    full_script += ' or '
                                full_script += feb_script
                            else :
                                feb_script=' '
                            if 'Mart' in ss2021:
                                if full_script:
                                    full_script += ' or '
                                mar_script=f'''  {timepar} between \'{year}-03-01\' and \'{year}-04-01\' '''
                                full_script += mar_script
                            else :
                                mar_script=' '
                            if 'Aprel' in ss2021:
                                if full_script:
                                    full_script += ' or '
                                apr_script=f'''  {timepar} between \'{year}-04-01\' and \'{year}-05-01\' '''
                                full_script += apr_script
                            else :
                                apr_script=' '
                            if 'May' in ss2021:
                                if full_script:
                                    full_script += ' or '
                                may_script=f'''  {timepar} between \'{year}-05-01\' and \'{year}-06-01\' '''
                                full_script += may_script
                            else :
                                may_script=' '
                            if 'İyun' in ss2021:
                                if full_script:
                                    full_script += ' or '
                                jun_script=f'''  {timepar} between \'{year}-06-01\' and \'{year}-07-01\' '''
                                full_script += jun_script
                            else :
                                jun_script=' '
                            if 'İyul' in ss2021:
                                if full_script:
                                    full_script += ' or '
                                jul_script=f'''  {timepar} between \'{year}-07-01\' and \'{year}-08-01\' '''
                                full_script += jul_script
                            else :
                                jul_script=' '
                            if 'Avqust' in ss2021:
                                if full_script:
                                    full_script += ' or '
                                avg_script=f'''  {timepar} between \'{year}-08-01\' and \'{year}-09-01\' '''
                                full_script += avg_script
                            else :
                                avg_script=' '
                            if 'Sentyabr' in ss2021:
                                if full_script:
                                    full_script += ' or '
                                sen_script=f'''  {timepar} between \'{year}-09-01\' and \'{year}-10-01\' '''
                                full_script += sen_script
                            else :
                                sen_script=' '
                            if 'Oktyabr' in ss2021:
                                if full_script:
                                    full_script += ' or '
                                oct_script=f'''  {timepar} between \'{year}-10-01\' and \'{year}-11-01\' '''
                                full_script += oct_script
                            else :
                                oct_script=' '
                            if 'Noyabr' in ss2021:
                                if full_script:
                                    full_script += ' or '
                                nov_script=f'''  {timepar} between \'{year}-11-01\' and \'{year}-12-01\' '''
                                full_script += nov_script
                            else :
                                nov_script=' '
                            if 'Dekabr' in ss2021:
                                if full_script:
                                    full_script += ' or '
                                dec_script=f'''  {timepar} between \'{year}-12-01\' and \'{year+1}-01-01\' '''
                                full_script += dec_script
                            else :
                                dec_script=' '
                    if s2022:
                        if ss2022!= ' ':
                            #full_script = ''   
                            year=2022
                            if 'Yanvar' in ss2022:
                                if full_script:
                                    full_script += ' or '
                                jan_script=f'''  {timepar} between \'{year}-01-01\' and \'{year}-02-01\' '''
                                full_script += jan_script
                            else :
                                jan_script=' '
                            if 'Fevral' in ss2022:
                                feb_script=f'''  {timepar} between \'{year}-02-01\' and \'{year}-03-01\' '''
                                if full_script:
                                    full_script += ' or '
                                full_script += feb_script
                            else :
                                feb_script=' '
                            if 'Mart' in ss2022:
                                if full_script:
                                    full_script += ' or '
                                mar_script=f'''  {timepar} between \'{year}-03-01\' and \'{year}-04-01\' '''
                                full_script += mar_script
                            else :
                                mar_script=' '
                            if 'Aprel' in ss2022:
                                if full_script:
                                    full_script += ' or '
                                apr_script=f'''  {timepar} between \'{year}-04-01\' and \'{year}-05-01\' '''
                                full_script += apr_script
                            else :
                                apr_script=' '
                            if 'May' in ss2022:
                                if full_script:
                                    full_script += ' or '
                                may_script=f'''  {timepar} between \'{year}-05-01\' and \'{year}-06-01\' '''
                                full_script += may_script
                            else :
                                may_script=' '
                            if 'İyun' in ss2022:
                                if full_script:
                                    full_script += ' or '
                                jun_script=f'''  {timepar} between \'{year}-06-01\' and \'{year}-07-01\' '''
                                full_script += jun_script
                            else :
                                jun_script=' '
                            if 'İyul' in ss2022:
                                if full_script:
                                    full_script += ' or '
                                jul_script=f'''  {timepar} between \'{year}-07-01\' and \'{year}-08-01\' '''
                                full_script += jul_script
                            else :
                                jul_script=' '
                            if 'Avqust' in ss2022:
                                if full_script:
                                    full_script += ' or '
                                avg_script=f'''  {timepar} between \'{year}-08-01\' and \'{year}-09-01\' '''
                                full_script += avg_script
                            else :
                                avg_script=' '
                            if 'Sentyabr' in ss2022:
                                if full_script:
                                    full_script += ' or '
                                sen_script=f'''  {timepar} between \'{year}-09-01\' and \'{year}-10-01\' '''
                                full_script += sen_script
                            else :
                                sen_script=' '
                            if 'Oktyabr' in ss2022:
                                if full_script:
                                    full_script += ' or '
                                oct_script=f'''  {timepar} between \'{year}-10-01\' and \'{year}-11-01\' '''
                                full_script += oct_script
                            else :
                                oct_script=' '
                            if 'Noyabr' in ss2022:
                                if full_script:
                                    full_script += ' or '
                                nov_script=f'''  {timepar} between \'{year}-11-01\' and \'{year}-12-01\' '''
                                full_script += nov_script
                            else :
                                nov_script=' '
                            if 'Dekabr' in ss2022:
                                if full_script:
                                    full_script += ' or '
                                dec_script=f'''  {timepar} between \'{year}-12-01\' and \'{year+1}-01-01\' '''
                                full_script += dec_script
                            else :
                                dec_script=' '
                    if s2023:
                        if ss2023!= ' ':
                            #full_script = ''   
                            year=2023
                            if 'Yanvar' in ss2023:
                                if full_script:
                                    full_script += ' or '
                                jan_script=f'''  {timepar} between \'{year}-01-01\' and \'{year}-02-01\' '''
                                full_script += jan_script
                            else :
                                jan_script=' '
                            if 'Fevral' in ss2023:
                                feb_script=f'''  {timepar} between \'{year}-02-01\' and \'{year}-03-01\' '''
                                if full_script:
                                    full_script += ' or '
                                full_script += feb_script
                            else :
                                feb_script=' '
                            if 'Mart' in ss2023:
                                if full_script:
                                    full_script += ' or '
                                mar_script=f'''  {timepar} between \'{year}-03-01\' and \'{year}-04-01\' '''
                                full_script += mar_script
                            else :
                                mar_script=' '
                            if 'Aprel' in ss2023:
                                if full_script:
                                    full_script += ' or '
                                apr_script=f'''  {timepar} between \'{year}-04-01\' and \'{year}-05-01\' '''
                                full_script += apr_script
                            else :
                                apr_script=' '
                            if 'May' in ss2023:
                                if full_script:
                                    full_script += ' or '
                                may_script=f'''  {timepar} between \'{year}-05-01\' and \'{year}-06-01\' '''
                                full_script += may_script
                            else :
                                may_script=' '
                            if 'İyun' in ss2023:
                                if full_script:
                                    full_script += ' or '
                                jun_script=f'''  {timepar} between \'{year}-06-01\' and \'{year}-07-01\' '''
                                full_script += jun_script
                            else :
                                jun_script=' '
                            if 'İyul' in ss2023:
                                if full_script:
                                    full_script += ' or '
                                jul_script=f'''  {timepar} between \'{year}-07-01\' and \'{year}-08-01\' '''
                                full_script += jul_script
                            else :
                                jul_script=' '
                            if 'Avqust' in ss2023: 
                                if full_script:
                                    full_script += ' or '
                                avg_script=f'''  {timepar} between \'{year}-08-01\' and \'{year}-09-01\' '''
                                full_script += avg_script
                            else :
                                avg_script=' '
                            if 'Sentyabr' in ss2023:
                                if full_script:
                                    full_script += ' or '
                                sen_script=f'''  {timepar} between \'{year}-09-01\' and \'{year}-10-01\' '''
                                full_script += sen_script
                            else :
                                sen_script=' '
                            if 'Oktyabr' in ss2023:
                                if full_script:
                                    full_script += ' or '
                                oct_script=f'''  {timepar} between \'{year}-10-01\' and \'{year}-11-01\' '''
                                full_script += oct_script
                            else :
                                oct_script=' '
                            if 'Noyabr' in ss2023:
                                if full_script:
                                    full_script += ' or '
                                nov_script=f'''  {timepar} between \'{year}-11-01\' and \'{year}-12-01\' '''
                                full_script += nov_script
                            else :
                                nov_script=' '
                            if 'Dekabr' in ss2023:
                                if full_script:
                                    full_script += ' or '
                                dec_script=f'''  {timepar} between \'{year}-12-01\' and \'{year+1}-01-01\' '''
                                full_script += dec_script
                            else :
                                dec_script=' '
                    

                        
                    ss,dd=st.sidebar.columns(2)
                    last_fifteen=ss.checkbox("Son 15 gün")
                    last_thirty=dd.checkbox("Son 30 gün")
                    cf=st.sidebar.button("Filterləri təmizlə")
                            
                    if  last_fifteen:
                        if full_script:
                            full_script += ' or '
                        start_date,end_date=funcc.getlast15date()
                        lst15=f'''{timepar} between  \'{start_date}\' and \'{end_date}\''''
                        full_script += lst15
                    else :
                        last_fifteen=False
                        lst15=False
                                    #st.write(start_date,end_date)
                    if  last_thirty:
                        if full_script:
                            full_script += ' or '
                        start_date,end_date=funcc.getlast30date()
                        lst30=f'''{timepar} between  \'{start_date}\' and \'{end_date}\''''
                        full_script += lst30
                    else: 
                        last_thirty=False
                        lst30=False
                    
                    if  cf == False :
                        cf=False
                        clrfltr=' '
                    else :
                        start_date,end_date=funcc.getdate()
                        #st.write(start_date,end_date)
                        full_script=f'''  {timepar} between  \'{start_date}\' and \'{end_date}\' ''' 
                        lst30=True  
                        lst15=True
                    if full_script:
                        scripts = f" ({full_script.strip()})"
                    else :
                        start_date,end_date=funcc.getdate()
                        #st.write(start_date,end_date)
                        scripts=f'''  {timepar} between  \'{start_date}\' and \'{end_date}\' '''
                        
                else :
                    sd,ds=st.sidebar.columns(2)
                    twenty1=sd.checkbox("2020")#,on_click=funcc.twenty)
                    twentytone1=ds.checkbox("2021")#,on_click=funcc.twentyone)
                    twentytwo1=sd.checkbox("2022")#,on_click=funcc.twentytwo)
                    twentythree1=ds.checkbox("2023")#,on_click=funcc.twentythree)
                
                current_month=datetime.now().month
                if  g and twenty1:
                    start_date,end_date=funcc.twenty()
                    a20=f'''{timepar} between  \'{start_date}\' and \'{end_date}\''''
                else :
                    a20=False
                if  g and twentytone1:
                    start_date,end_date=funcc.twentyone()
                    a21=f'''{timepar} between  \'{start_date}\' and \'{end_date}\''''
                else :
                    a21=False
                if  g and twentytwo1:
                    start_date,end_date=funcc.twentytwo()
                    a22=f'''{timepar} between  \'{start_date}\' and \'{end_date}\''''
                else :
                    a22=False
                if  g and twentythree1:
                    start_date,end_date=funcc.twentythree()
                    a23=f'''{timepar} between  \'{start_date}\' and \'{end_date}\' '''
                else :
                    a23=False
                if  g and twenty1==False and twentytone1==False  and twentytwo1==False and twentythree1==False :
                    start_date,end_date=funcc.twentythree()
                    a23=f''' {timepar} between  \'{start_date}\' and \'{end_date}\' '''
                else : 
                    dd=False
                
                
                mpayy_con=st.container()
                modd_con=st.container()
                abb_con=st.container()
                if selected2=='Mpay terminal':
                    if mpayy_con:
                        if g is False:
                            
                            st.header (str(selected2)) 
                            
                            yrd=st.sidebar.button('Yarat')
                            if yrd:                
                                SQl_query_filtered=mpay_terminal(conn,scripts)
                                SQl_query_filtered = SQl_query_filtered.fillna('')
                                a=pd.DataFrame(SQl_query_filtered)
                                    
                                    
                                        
                                numeric_cols=['totalcount','totalamount','rejectcount','rejectamount']
                                            
                                result = pd.DataFrame([{col: int(a[col].sum()) for col in numeric_cols}]).T
                                            
                                result = result.reset_index(drop=True).T
                                result.columns=['totalcount','totalamount','rejectcount','rejectamount']
                                result = result.reset_index()
                                            
                                result.columns=[' ','totalcount','sum(totalamount)','rejectcount','sum(rejectamount)']
                                #result["count"]=pd.DataFrame(a.count()).iloc[0,0]
                                result.iloc[0,0]='Total'
                                #result = result.set_index('Total')
                                funcc.dataframee1(result)
                                funcc.dataframee(SQl_query_filtered)
                                csv = funcc.convert_df(SQl_query_filtered)
                                st.sidebar.header("Yüklə:")
                                st.sidebar.download_button(label="CSV",data=csv,file_name=selected2+' '+str(datetime.strftime(start_date, '%Y.%m.%d'))+'-'+str(datetime.strftime(end_date, '%Y.%m.%d')) +'.csv',mime='text/csv'    )
                                st.sidebar.download_button(label="TEXT",data=csv,file_name=selected2+' '+str(datetime.strftime(start_date, '%Y.%m.%d'))+'-'+str(datetime.strftime(end_date, '%Y.%m.%d')) +'.txt',mime='text')
                                #excel=funcc.to_excel(SQl_query_filtered)
                                if len(SQl_query_filtered.index)>1000000:
                                    excel=funcc.to_excell(SQl_query_filtered)
                                    st.sidebar.download_button(label="EXCEL",data=excel,file_name=selected2+' '+str(datetime.strftime(start_date, '%Y.%m.%d'))+'-'+str(datetime.strftime(end_date, '%Y.%m.%d')) +'.xlsx',mime='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
                                else:
                                    excel=funcc.to_excel(SQl_query_filtered)
                                    st.sidebar.download_button(label="EXCEL",data=excel,file_name=selected2+' '+str(datetime.strftime(start_date, '%Y.%m.%d'))+'-'+str(datetime.strftime(end_date, '%Y.%m.%d')) +'.xlsx',mime='xlsx')
                        else :
                           
                            st.header (str(selected2) )
                            
                            yrd=st.button('Yarat')
                            if yrd:               
                                SQl_query_filtered=mpay_terminal_group(conn,dd,a20,a21,a22,a23)
                                SQl_query_filtered = SQl_query_filtered.fillna('')
                                a=pd.DataFrame(SQl_query_filtered)
                                    
                                    
                                        
                                numeric_cols=['totalcount','totalamount','rejectcount','rejectamount']
                                            
                                result = pd.DataFrame([{col: int(a[col].sum()) for col in numeric_cols}]).T
                                            
                                result = result.reset_index(drop=True).T
                                result.columns=['totalcount','totalamount','rejectcount','rejectamount']
                                result = result.reset_index()
                                            
                                result.columns=[' ','totalcount','sum(totalamount)','rejectcount','sum(rejectamount)']
                                #result["count"]=pd.DataFrame(a.count()).iloc[0,0]
                                result.iloc[0,0]='Total'
                                #result = result.set_index('Total')
                                funcc.dataframee1(result)
                                funcc.dataframee(SQl_query_filtered)
                                csv = funcc.convert_df(SQl_query_filtered)
                                st.sidebar.header("Yüklə:")
                                st.sidebar.download_button(label="CSV",data=csv,file_name=selected2+' '+str(datetime.strftime(start_date, '%Y.%m.%d'))+'-'+str(datetime.strftime(end_date, '%Y.%m.%d')) +'.csv',mime='text/csv'    )
                                st.sidebar.download_button(label="TEXT",data=csv,file_name=selected2+' '+str(datetime.strftime(start_date, '%Y.%m.%d'))+'-'+str(datetime.strftime(end_date, '%Y.%m.%d')) +'.txt',mime='text')
                                #excel=funcc.to_excel(SQl_query_filtered)
                                if len(SQl_query_filtered.index)>1000000:
                                    excel=funcc.to_excell(SQl_query_filtered)
                                    st.sidebar.download_button(label="EXCEL",data=excel,file_name=selected2+' '+str(datetime.strftime(start_date, '%Y.%m.%d'))+'-'+str(datetime.strftime(end_date, '%Y.%m.%d')) +'.xlsx',mime='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
                                else:
                                    excel=funcc.to_excel(SQl_query_filtered)
                                    st.sidebar.download_button(label="EXCEL",data=excel,file_name=selected2+' '+str(datetime.strftime(start_date, '%Y.%m.%d'))+'-'+str(datetime.strftime(end_date, '%Y.%m.%d')) +'.xlsx',mime='xlsx')
                elif selected2=='Modenis terminal':
                    if modd_con:
                        if g is False :
                            yrd=st.sidebar.button('Yarat')
                            if yrd: 
                                st.header (str(selected2) )
                                                
                                SQl_query_filtered=modenis_terminal(conn,scripts)
                                SQl_query_filtered = SQl_query_filtered.fillna('')
                                a=pd.DataFrame(SQl_query_filtered)
                                    
                                    
                                        
                                numeric_cols=['PayCount','PayAmount','Profit','RejectAmount','RejectCount']
                                            
                                result = pd.DataFrame([{col: int(a[col].sum()) for col in numeric_cols}]).T
                                            
                                result = result.reset_index(drop=True).T
                                result.columns=['Paycount','PayAmount','Profit','RejectAmount','RejectCount']
                                result = result.reset_index()
                                            
                                result.columns=[' ','Paycount','sum(PayAmount)','sum(Profit)','sum(RejectAmount)','RejectCount']
                                #result["count"]=pd.DataFrame(a.count()).iloc[0,0]
                                result.iloc[0,0]='Total'
                                #result = result.set_index('Total')
                                funcc.dataframee1(result)
                                funcc.dataframee(SQl_query_filtered)
                                csv = funcc.convert_df(SQl_query_filtered)
                                st.sidebar.header("Yüklə:")
                                st.sidebar.download_button(label="CSV",data=csv,file_name=selected2+' '+str(datetime.strftime(start_date, '%Y.%m.%d'))+'-'+str(datetime.strftime(end_date, '%Y.%m.%d')) +'.csv',mime='text/csv'    )
                                st.sidebar.download_button(label="TEXT",data=csv,file_name=selected2+' '+str(datetime.strftime(start_date, '%Y.%m.%d'))+'-'+str(datetime.strftime(end_date, '%Y.%m.%d')) +'.txt',mime='text')
                                    #excel=funcc.to_excel(SQl_query_filtered)
                                if len(SQl_query_filtered.index)>1000000:
                                    excel=funcc.to_excell(SQl_query_filtered)
                                    st.sidebar.download_button(label="EXCEL",data=excel,file_name=selected2+' '+str(datetime.strftime(start_date, '%Y.%m.%d'))+'-'+str(datetime.strftime(end_date, '%Y.%m.%d')) +'.xlsx',mime='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
                                else:
                                    excel=funcc.to_excel(SQl_query_filtered)
                                    st.sidebar.download_button(label="EXCEL",data=excel,file_name=selected2+' '+str(datetime.strftime(start_date, '%Y.%m.%d'))+'-'+str(datetime.strftime(end_date, '%Y.%m.%d')) +'.xlsx',mime='xlsx')
                        else :
                            yrd=st.sidebar.button('Yarat')
                            if yrd: 
                                st.header (str(selected2) )
                                                
                                SQl_query_filtered= modenis_terminal_group_check(conn,dd,a20,a21,a22,a23)
                                SQl_query_filtered = SQl_query_filtered.fillna('')
                                a=pd.DataFrame(SQl_query_filtered)
                                    
                                    
                                        
                                numeric_cols=['PayCount','PayAmount','Profit','RejectAmount','RejectCount']
                                            
                                result = pd.DataFrame([{col: int(a[col].sum()) for col in numeric_cols}]).T
                                            
                                result = result.reset_index(drop=True).T
                                result.columns=['Paycount','PayAmount','Profit','RejectAmount','RejectCount']
                                result = result.reset_index()
                                            
                                result.columns=[' ','Paycount','sum(PayAmount)','sum(Profit)','sum(RejectAmount)','RejectCount']
                                #result["count"]=pd.DataFrame(a.count()).iloc[0,0]
                                result.iloc[0,0]='Total'
                                #result = result.set_index('Total')
                                funcc.dataframee1(result)
                                funcc.dataframee(SQl_query_filtered)
                                csv = funcc.convert_df(SQl_query_filtered)
                                st.sidebar.header("Yüklə:")
                                st.sidebar.download_button(label="CSV",data=csv,file_name=selected2+' '+str(datetime.strftime(start_date, '%Y.%m.%d'))+'-'+str(datetime.strftime(end_date, '%Y.%m.%d')) +'.csv',mime='text/csv'    )
                                st.sidebar.download_button(label="TEXT",data=csv,file_name=selected2+' '+str(datetime.strftime(start_date, '%Y.%m.%d'))+'-'+str(datetime.strftime(end_date, '%Y.%m.%d')) +'.txt',mime='text')
                                #excel=funcc.to_excel(SQl_query_filtered)
                                if len(SQl_query_filtered.index)>1000000:
                                    excel=funcc.to_excell(SQl_query_filtered)
                                    st.sidebar.download_button(label="EXCEL",data=excel,file_name=selected2+' '+str(datetime.strftime(start_date, '%Y.%m.%d'))+'-'+str(datetime.strftime(end_date, '%Y.%m.%d')) +'.xlsx',mime='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
                                else:
                                    excel=funcc.to_excel(SQl_query_filtered)
                                    st.sidebar.download_button(label="EXCEL",data=excel,file_name=selected2+' '+str(datetime.strftime(start_date, '%Y.%m.%d'))+'-'+str(datetime.strftime(end_date, '%Y.%m.%d')) +'.xlsx',mime='xlsx')
                elif selected2=='ABB Korporativ hesab':
                    if abb_con:
                        if g is False :
                            yrd=st.sidebar.button('Yarat')
                            if yrd: 
                                st.header (str(selected2) )
                                                    
                                SQl_query_filtered=abb(conn,scripts)
                                SQl_query_filtered = SQl_query_filtered.fillna('')
                                a=pd.DataFrame(SQl_query_filtered)
                                    
                                    
                                        
                                numeric_cols=['sum_income','sum_outcome']
                                            
                                result = pd.DataFrame([{col: int(a[col].sum()) for col in numeric_cols}]).T
                                            
                                result = result.reset_index(drop=True).T
                                result.columns=['sum_income','sum_outcome']
                                result = result.reset_index()
                                            
                                result.columns=[' ','sum(sum_income)','sum(sum_outcome)']
                                result["count"]=pd.DataFrame(a.count()).iloc[0,0]
                                result.iloc[0,0]='Total'
                                #result = result.set_index('Total')
                                funcc.dataframee1(result)
                                funcc.dataframee(SQl_query_filtered)
                                csv = funcc.convert_df(SQl_query_filtered)
                                st.sidebar.header("Yüklə:")
                                st.sidebar.download_button(label="CSV",data=csv,file_name=selected2 +'.csv',mime='text/csv'    )
                                st.sidebar.download_button(label="TEXT",data=csv,file_name=selected2+'.txt',mime='text')
                                excel=funcc.to_excel(SQl_query_filtered)
                                    #excel=funcc.to_excel(SQl_query_filtered)
                                if len(SQl_query_filtered.index)>1000000:
                                    excel=funcc.to_excell(SQl_query_filtered)
                                    st.sidebar.download_button(label="EXCEL",data=excel,file_name=selected2+'.xlsx',mime='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
                                else:
                                    excel=funcc.to_excel(SQl_query_filtered)
                                    st.sidebar.download_button(label="EXCEL",data=excel,file_name=selected2+'.xlsx',mime='xlsx')
                        else:
                            yrd=st.sidebar.button('Yarat')
                            if yrd: 
                                st.header (str(selected2) )
                                                
                                SQl_query_filtered=abb_group(conn,dd,a20,a21,a22,a23)
                                SQl_query_filtered = SQl_query_filtered.fillna('')
                                a=pd.DataFrame(SQl_query_filtered)
                                    
                                    
                                        
                                numeric_cols=['sum_income','count']
                                            
                                result = pd.DataFrame([{col: int(a[col].sum()) for col in numeric_cols}]).T
                                            
                                result = result.reset_index(drop=True).T
                                result.columns=['sum_income','count']
                                result = result.reset_index()
                                            
                                result.columns=[' ','sum(sum_income)','count']
                                #result["count"]=pd.DataFrame(a.count()).iloc[0,0]
                                result.iloc[0,0]='Total'
                                #result = result.set_index('Total')
                                funcc.dataframee1(result)
                                funcc.dataframee(SQl_query_filtered)
                                csv = funcc.convert_df(SQl_query_filtered)
                                st.sidebar.header("Yüklə:")
                                st.sidebar.download_button(label="CSV",data=csv,file_name=selected2+' '+str(datetime.strftime(start_date, '%Y.%m.%d'))+'-'+str(datetime.strftime(end_date, '%Y.%m.%d')) +'.csv',mime='text/csv'    )
                                st.sidebar.download_button(label="TEXT",data=csv,file_name=selected2+' '+str(datetime.strftime(start_date, '%Y.%m.%d'))+'-'+str(datetime.strftime(end_date, '%Y.%m.%d')) +'.txt',mime='text')
                                #excel=funcc.to_excel(SQl_query_filtered)
                                if len(SQl_query_filtered.index)>1000000:
                                    excel=funcc.to_excell(SQl_query_filtered)
                                    st.sidebar.download_button(label="EXCEL",data=excel,file_name=selected2+' '+str(datetime.strftime(start_date, '%Y.%m.%d'))+'-'+str(datetime.strftime(end_date, '%Y.%m.%d')) +'.xlsx',mime='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
                                else:
                                    excel=funcc.to_excel(SQl_query_filtered)
                                    st.sidebar.download_button(label="EXCEL",data=excel,file_name=selected2+' '+str(datetime.strftime(start_date, '%Y.%m.%d'))+'-'+str(datetime.strftime(end_date, '%Y.%m.%d')) +'.xlsx',mime='xlsx')
        else :
            st.markdown(
    """
    <style>
        div [data-testid=stImage]{
            text-align: center;
            display: block;
            margin-left: auto;
            margin-right: auto;
            width: 100%;
            margin-top: 10%;
        }

    </style>
    """, unsafe_allow_html=True
)
            icon=Image.open('edatamart.png')        
            st.image(icon)    
                
            
               
                                    
                        
                                                


                                
                    
                        


if __name__ == '__main__':
    
    main()
    st.write('<style>div.row-widget.stRadio > div{flex-direction:row;justify-content: center;} </style>', unsafe_allow_html=True)

        
    st.markdown("""

<style>
    footer {
    
          visibility: hidden;
    
      }
    footer:after {
          content:'Property of Emanat'; 
          visibility: visible;
          display: block;
          position: relative;
          #background-color: red;
          padding: 5px;
          top: 2px;
    }

    .appview-container .main .block-container {

        padding-top: 0rem !important;

        padding-right: 0.5rem !important;

        padding-left: 0.5rem !important;

        padding-bottom: 0rem !important;

    }
    ::-webkit-scrollbar {
        background: rgb(28 78 128 / 48%) !important;
        border-radius: 100px !important;
        height: 6px !important;
        width: 10.5px !important;
    }
    
    span[data-baseweb="tag"] {
        background-color: blue !important;
    }
    # p {
    #     font-size: 13px;
    # }
    # html, body, [class*="css"]  {


    #     font-size: 13px;

    # }


   

</style>

""" , unsafe_allow_html=True)
      
       


