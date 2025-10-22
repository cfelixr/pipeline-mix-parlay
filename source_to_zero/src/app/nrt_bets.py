import os
import sys
from datetime import datetime
import time
import polars as pl
import gc

import config
import aws.s3 as s3
import utils.helpers as h
import utils.manage_information as m_inf

# Getting the enviroment variables

print("Version Code V.0.2.0")
print(f'Script execution Id: {os.getpid()}')

print(f'bucket: {config.BUCKET_TARGET}')
print(f'object key: {config.OBJECT_KEY}')

# Searching the last timestamp
print(f'INIT: SEARCH LAST PARQUET')

files = s3.get_objects(config.BUCKET_TARGET, config.OBJECT_KEY)
max_modify_date, max_tstamp = m_inf.initialization_timestamp(files, sys.argv)
print(f'INIT-FINISHED: SEARCH LAST PARQUET')

DELAY_TIME = config.DELAY_TIME
print(f'DELAY TIME: {DELAY_TIME} minutes')

brief_writter = lambda data, start_date, end_date, tstamp: m_inf.write_to_s3(data, config.BETS_ONLINE_SCHEMA, start_date, end_date, config.BUCKET_TARGET, config.OBJECT_KEY, tstamp)

try:
    while True:
        total_bets_data_current_day = []
        total_bets_data_next_day    = []

        initial_max_tstamp  = max_tstamp
        initial_modify_date = max_modify_date

        start_time = time.time()
        print("Start Fetching iteration ...")

        try:
            while True:
                bets_data, new_max_tstamp, maxMobiusModifiedOn = m_inf.fetch_bets_data_by_timestamp(config.ROOT_API_ENDPOINT, max_tstamp)
                new_max_tstamp = h.format_tstamp(new_max_tstamp)

                print("Time fetching information: %s  seconds" % (time.time() - start_time))
                print(f'{len(bets_data)}, ts:{max_tstamp}, {new_max_tstamp}')

                if not ((new_max_tstamp != "0x" or len(new_max_tstamp) > 2) and m_inf.is_old_enough(maxMobiusModifiedOn, DELAY_TIME)):
                    print(f"Non recieve new data or not is at least {DELAY_TIME} minutes old. Delay 60s")
                    del bets_data
                    gc.collect()
                    time.sleep(60)
                    continue
                if new_max_tstamp == max_tstamp:
                    print("Non recieve new data. Delay 60s")
                    time.sleep(60)
                    continue
                # Only usefull when it runs the first time
                if not bets_data:
                    break

                # Preprocessing data
                print('Preprocessing data' )
                try:
                    bets_data           = h.preprocess_data(bets_data, config.FIELDS_TO_KEEP)
                    new_max_modify_date = max({x['modifyDate'] for x in bets_data}, key=lambda x: datetime.strptime(h.format_date(x), "%Y-%m-%dT%H:%M:%S.%f"))

                    
                    print(f'modifyDate: {initial_modify_date}, {max_modify_date}, {new_max_modify_date}')

                    if not initial_modify_date:
                        initial_modify_date = bets_data[0]['modifyDate']

                    # Splitting the data in current and next day information
                    bets_current_day = [y for y in bets_data if y['modifyDate'][:10] <= initial_modify_date[:10]]
                    bets_next_day    = [y for y in bets_data if y['modifyDate'][:10] > initial_modify_date[:10]]
                    total_bets_data_current_day.extend(bets_current_day)
                    total_bets_data_next_day.extend(bets_next_day)

                    # Setting the state variables
                    max_tstamp      = new_max_tstamp
                    max_modify_date = new_max_modify_date

                    # To control the max lenght of batch information.
                    if len(total_bets_data_current_day) + len(total_bets_data_next_day) > 4999:
                        break
                except Exception as e:
                    print("Preprocessing Error. Please check what happend Exactly")
                    print(e)
                    exit()

            if not (total_bets_data_current_day or total_bets_data_next_day):
                time.sleep(5)
            else:
                try:
                    if (initial_modify_date[:10] == max_modify_date[:10]):
                        brief_writter(
                            total_bets_data_current_day,
                            initial_modify_date,
                            max_modify_date,
                            max_tstamp
                        )
                    else:
                        if not total_bets_data_current_day:
                            brief_writter(
                                total_bets_data_next_day,
                                f'{max_modify_date[:10]}T00:00:00.000',
                                max_modify_date,
                                max_tstamp
                            )
                        else:
                            brief_writter(
                                total_bets_data_current_day,
                                initial_modify_date,
                                f'{initial_modify_date[:10]}T23:59:59.999',
                                max_tstamp
                            )
                            brief_writter(
                                total_bets_data_next_day,
                                f'{max_modify_date[:10]}T00:00:00.000',
                                max_modify_date,
                                max_tstamp
                            )
                except Exception as e:
                    print("Error saving the information. Please check the problem.")
                    print(e)
                    exit()

            print("Time until save information: %s seconds" % (time.time() - start_time))
        except Exception as e:
            max_modify_date = initial_modify_date
            print(e)

except Exception as e:
    print(e)
finally:
    print('Script Error. Please check the logs')
    raise SystemError("Error unknown. please check the logs")