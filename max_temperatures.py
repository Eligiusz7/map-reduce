# max temperatures on weather stations

from mrjob.job import MRJob


class MRMaxTemperature(MRJob):
    def make_celsius(self, tenths_celsius):
        return float(tenths_celsius) / 10

    def mapper(self, _, line):
        (location, date, type, temperature, x, y, z, w) = line.split(',')
        if type == 'TMAX':
            temperature = self.make_celsius(temperature)
            yield location, temperature

    def reducer(self, location, temperature):
        yield location, max(temperature)


if __name__ == '__main__':
    MRMaxTemperature.run()
