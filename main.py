import API_data_collector
import detector
import Report_builder
import sys

def main():
    
    API_data_collector.main('delta', 16, '2099-09-17 00:00:00', '2099-10-02 23:59:59' )
    detector.main()
    Report_builder.main("delta")
    
if __name__ == "__main__":
    main()
