# words frequency in a book

from mrjob.job import MRJob
import re

WORD_REGEXP = re.compile(r'[\w]+')


class MRWordFrequencyCount(MRJob):
    def mapper(self, _, line):
        words = WORD_REGEXP.findall(line)
        for word in words:
            yield word.lower(), 1

    def reducer(self, word, occurrences):
        yield word, sum(occurrences)


if __name__ == '__main__':
    MRWordFrequencyCount.run()
