# words frequency in a book with sorted order by word

from mrjob.job import MRJob
from mrjob.step import MRStep
import re

WORD_REGEXP = re.compile(r'[\w]+')


class MRWordFrequencyCount(MRJob):
    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_words,
                   reducer=self.reducer_count_words),
            MRStep(mapper=self.mapper_make_counts_key,
                   reducer=self.reducer_output_words)
        ]

    def mapper_get_words(self, _, line):
        words = WORD_REGEXP.findall(line)
        for word in words:
            yield word.lower(), 1

    def reducer_count_words(self, word, occurrences):
        yield word, sum(occurrences)

    def mapper_make_counts_key(self, word, count):
        yield f'{int(count):04}', word

    def reducer_output_words(self, count, words):
        for word in words:
            yield count, word


if __name__ == '__main__':
    MRWordFrequencyCount.run()
