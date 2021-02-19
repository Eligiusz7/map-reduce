from mrjob.job import MRJob


class MRRatingCounter(MRJob):
    def mapper(self, key, line):
        (user_id, movie_id, rating, timestamp) = line.split('\t')
        yield rating, 1

    def reducer(self, rating, occurrences):
        yield rating, sum(occurrences)


if __name__ == '__main__':
    MRRatingCounter.run()
