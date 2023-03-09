import API_data_collector_IBM
import detector
import Report_builder_IBM_usage
import sys
import datetime
import calendar
from pprint import pprint

def main():
    
    weeks = [{'start':'2023-01-30 00:00:00', 'end':'2023-02-05 23:59:59'},
             {'start':'2023-02-06 00:00:00', 'end':'2023-02-12 23:59:59'},
             {'start':'2023-02-13 00:00:00', 'end':'2023-02-19 23:59:59'},
             {'start':'2023-02-20 00:00:00', 'end':'2023-02-26 23:59:59'},
             {'start':'2023-02-27 00:00:00', 'end':'2023-03-05 23:59:59'},
             {'start':'2023-03-06 00:00:00', 'end':'2023-03-12 23:59:59'}]

    for week in weeks:
        API_data_collector_IBM.main('date', 16, week['start'], week['end'] )
        detector.main()
        Report_builder_IBM_usage.main("delta")
        
if __name__ == "__main__":
    main()
