from datetime import datetime

class DatetimeManager:
    @staticmethod
    def to_datetime(time):
        return datetime.fromtimestamp(time / 1000) 
    
    @staticmethod
    def from_datetime(time):
        return int(time.timestamp() * 1000)