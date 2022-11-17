import boto3
import pandas as pd
import io
import datetime
from datetime import timedelta

# loop over date range
def daterange(start_date, end_date):
    for n in range(int ((end_date - start_date).days)):
        yield start_date + timedelta(n)

def timestream_gold_writing(single_date):
    s3 = boto3.client('s3')
    client_s3 = boto3.resource('s3')
    paginator = s3.get_paginator('list_objects')
    client_timestream = boto3.client('timestream-write', region_name='eu-west-1')
    page_iterator = paginator.paginate(Bucket='bse-gold',Prefix='inverters_data/dt='+str(single_date))

    for page in page_iterator:
        
        all_objects = page['Contents']
        
        for obj in all_objects:
            
            file_name = obj['Key']
            
            if file_name.endswith('parquet'):
                
                buffer = io.BytesIO()
                object = client_s3.Object('bse-gold', file_name)
                object.download_fileobj(buffer)
                df = pd.read_parquet(buffer)
                df = df.replace('','0.0')
                df = df.fillna(0)
                
                for x in range(len(df.axes[0])):
                    
                    dimensions = [{'Name': 'data_source', 'Value': str(df.Data_Source[x])}, {'Name': 'local_site_id','Value': str(df.Local_site_id[x])}, {'Name': 'local_inverter_id','Value': str(df.Local_inverter_id[x])}]
                    date_time = datetime.datetime.strptime(df.Datetime[x], '%Y-%m-%d %H:%M:%S').strftime('%s')
                    timestamp_millisecond = date_time + "000"
                    
                    common_attributes = {
                        'Dimensions': dimensions,
                        'MeasureValueType': 'DOUBLE',
                        'Time': str(timestamp_millisecond)
                    }
                    
                    dc_voltage = {'MeasureName': 'dc_voltage','MeasureValue': str(round(float(df.DC_Voltage[x]),3))}
                    temperature = {'MeasureName': 'temperature','MeasureValue': str(round(float(df.Temperature[x]),3))}
                    active_power = {'MeasureName': 'active_power','MeasureValue': str(round(float(df.Active_Power[x]),3))}
                    ac_energy = {'MeasureName': 'ac_energy','MeasureValue': str(round(float(df.AC_Energy[x]),3))}
                    ac_current_l1 = {'MeasureName': 'ac_current_l1','MeasureValue': str(round(float(df.AC_Current_L1[x]),3))}
                    ac_voltage_l1 = {'MeasureName': 'ac_voltage_l1','MeasureValue': str(round(float(df.AC_Voltage_L1[x]),3))}
                    ac_current_l2 = {'MeasureName': 'ac_current_l2','MeasureValue': str(round(float(df.AC_Current_L2[x]),3))}
                    ac_voltage_l2 = {'MeasureName': 'ac_voltage_l2','MeasureValue': str(round(float(df.AC_Voltage_L2[x]),3))}
                    ac_current_l3 = {'MeasureName': 'ac_current_l3','MeasureValue': str(round(float(df.AC_Current_L3[x]),3))}
                    ac_voltage_l3 = {'MeasureName': 'ac_voltage_l3','MeasureValue': str(round(float(df.AC_Voltage_L3[x]),3))}
                    ac_current = {'MeasureName': 'ac_current','MeasureValue': str(round(float(df.AC_Current[x]),3))}
                    ac_voltage = {'MeasureName': 'ac_voltage','MeasureValue': str(round(float(df.AC_Voltage[x]),3))}
                    reactive_power = {'MeasureName': 'reactive_power','MeasureValue': str(round(float(df.Reactive_Power[x]),3))}
                    power_factor = {'MeasureName': 'power_factor','MeasureValue': str(round(float(df.Power_Factor[x]),3))}
                    ac_power = {'MeasureName': 'ac_power','MeasureValue': str(round(float(df.AC_Power[x]),3))}
                    dc_current = {'MeasureName': 'dc_current','MeasureValue': str(round(float(df.DC_Current[x]),3))}
                    dc_power = {'MeasureName': 'dc_power','MeasureValue': str(round(float(df.DC_Power[x]),3))}
                    account_id = {'MeasureName': 'account_id','MeasureValue': str(round(float(df.Account_id[x]),3))}
                    
                    records = [dc_voltage, temperature, active_power, ac_energy, ac_current_l1, ac_voltage_l1, ac_current_l2, ac_voltage_l2, ac_current_l3, ac_voltage_l3, ac_current, ac_voltage, reactive_power, power_factor, ac_power, dc_current, dc_power, account_id]
                    
                    try:    
                        result = client_timestream.write_records(DatabaseName="brightsource_gold", TableName="inverters_data", Records=records, CommonAttributes=common_attributes)
                    
                    except Exception as err:
                            print("Error:", err)
                            print("""


                            """)
                            print(err.response)
                            print("""

                            
                            """)
                            print(err.response["Error"])

                    print("WriteRecords Status: [%s]" % result['ResponseMetadata']['HTTPStatusCode'])
    
    
start_date = "2022-11-01"
end_date = "2022-11-14"
if (start_date is not None and  end_date is not None):
    start_date 	= datetime.datetime.strptime(start_date, '%Y-%m-%d').date()
    end_date 	= datetime.datetime.strptime(end_date, '%Y-%m-%d').date()

for single_date in daterange(start_date, end_date):
    timestream_gold_writing(single_date)