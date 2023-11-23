from enum import Enum, auto

class TimeInterval(Enum):
    INTERVAL_1MIN = '1m'
    INTERVAL_3MIN = '3m'
    INTERVAL_5MIN = '5m'
    INTERVAL_15MIN = '15m'
    INTERVAL_30MIN = '30m'
    INTERVAL_1H = '1h'
    INTERVAL_2H = '2h'
    INTERVAL_4H = '4h'
    INTERVAL_6H = '6h'
    INTERVAL_8H = '8h'
    INTERVAL_12H = '12h'
    INTERVAL_1D = '1d'
    INTERVAL_3D = '3d'
    INTERVAL_1W = '1w'
    INTERVAL_1M = '1M'

    def to_milliseconds(self):
        if 'min' in self.value:
            return int(self.value[:-1]) * 60 * 1000
        elif 'h' in self.value:
            return int(self.value[:-1]) * 60 * 60 * 1000
        elif 'd' in self.value:
            return int(self.value[:-1]) * 24 * 60 * 60 * 1000
        elif 'w' in self.value:
            return int(self.value[:-1]) * 7 * 24 * 60 * 60 * 1000
        elif 'M' in self.value:
            return int(self.value[:-1]) * 30 * 24 * 60 * 60 * 1000
        else:
            raise ValueError("Invalid interval format")