# words frequency in a book

from mrjob.job import MRJob


class MRWordFrequencyCount(MRJob):
    def mapper(self, _, line):
        words = line.split()
        for word in words:
            yield word.lower(), 1

    def reducer(self, word, occurrences):
        yield word, sum(occurrences)


if __name__ == '__main__':
    MRWordFrequencyCount.run()
