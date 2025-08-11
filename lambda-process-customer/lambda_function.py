import json
import base64
from datetime import datetime
from dateutil.relativedelta import relativedelta

def process_customer_json_data(customer_data):
    # Dropping the Record if the spending score is much lower(15) or much Higher(90) else if they are customer with us recently(within 3 years)
    if (customer_data['spending_score'] < 15 or customer_data['spending_score'] > 90 ) or customer_data['customer_since_years'] < 3:
        return 'Dropped', customer_data 

    # Processing/Transforming the required customer details before writing to s3
    customer_data.pop('partition_key')
    
    customer_data['customer_id'] = str(customer_data['customer_id'])
    
    customer_data['full_name'] = customer_data['first_name']+' '+customer_data['last_name']
    
    customer_data['prefixed_full_name'] = customer_data['name_prefix'] + ' ' + customer_data['first_name'] + ' ' + customer_data['last_name']
    
    customer_data['salary'] = customer_data['salary'].removeprefix('$')
    
    customer_data['occupation'] = "".join(customer_data['occupation'])
    
    customer_data['gender'] = "".join(customer_data['gender']) 
    
    emp_date = datetime.strptime(customer_data['date_of_birth'],'%d-%m-%Y')
    
    customer_data['date_of_birth'] = emp_date.strftime('%Y-%m-%d')

    customer_data['age'] = relativedelta(datetime.now(),emp_date).years

    spending_score_status_list = [{"minimum":15,"maximum":20,"spending_score_status":"Very Low"},
                                {"minimum":21,"maximum":30,"spending_score_status":"Low"},
                                {"minimum":31,"maximum":50,"spending_score_status":"Medium"},
                                {"minimum":51,"maximum":70,"spending_score_status":"High"},
                                {"minimum":71,"maximum":90,"spending_score_status":"Very High"}
                                ]
    output_spending_score_status = None
    spending_score = customer_data['spending_score'] 
    for score_detail in spending_score_status_list:
        minimum = score_detail['minimum']
        maximum = score_detail['maximum']
        spending_score_status = score_detail['spending_score_status']
        if spending_score >= minimum and spending_score <= maximum:
            output_spending_score_status = spending_score_status
            break 
    customer_data['spending_score_status'] = output_spending_score_status if output_spending_score_status else 'None'

    return 'Ok', customer_data

def lambda_handler(event, context):
    records = []
    for record in event['records']:
        # Data Extract - Base64 data decoded from Firehose bytes decoded to UTF-8 for JSON data manipulation
        payload = json.loads(base64.b64decode(record['data']).decode('utf-8'))
        # Data Trasnform
        result, data = process_customer_json_data(payload)

        # Data Loading - Encoding JSON to UTF-8 bytes to serliazed format of Base64 bytes for Firehose bytes decoded to UTF-8 for support in JSON
        # json.dumps is used since the Producer converts data from JSON to str before transmitting data
        output_record = {
            'recordId': record['recordId'],
            'result': result,
            'data': base64.b64encode(json.dumps(data).encode('utf-8')).decode('utf-8')
        }
        records.append(output_record)
    print('Successfully processed {} records.'.format(len(event['records'])))
    return {'records': records}
