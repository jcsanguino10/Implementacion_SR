from datetime import datetime

date_str = "6/13/2018 00:12:34 PM"
date_obj = datetime.strptime(date_str, "%m/%d/%Y %H:%M:%S %p")
formatted_date_str = date_obj.strftime("%m/%d/%Y %H:%M:%S %p")

print(formatted_date_str)