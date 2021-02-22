from mrjob.job import MRJob
from mrjob.step import MRStep


class MostPopularSuperHero(MRJob):
    def configure_args(self):
        super(MostPopularSuperHero, self).configure_args()
        self.add_file_arg('--names', help='Path to marvel-names.txt')

    def load_name(self):
        self.hero_names = {}

        with open("marvel_names.txt", encoding='ascii', errors='ignore') as f:
            for line in f:
                fields = line.split('"')
                hero_id = int(fields[0])
                self.hero_names[hero_id] = fields[1]

    def steps(self):
        return [
            MRStep(mapper=self.mapper_count_friends_per_line,
                   reducer=self.reducer_sum_friends),
            MRStep(mapper=self.mapper_prep_for_sort,
                   mapper_init=self.load_name,
                   reducer=self.reducer_find_max_friends)
        ]

    def mapper_count_friends_per_line(self, _, line):
        fields = line.split()
        hero_id = fields[0]
        num_friends = len(fields) - 1
        yield int(hero_id), int(num_friends)

    def reducer_sum_friends(self, hero_id, friends):
        yield hero_id, sum(friends)

    def mapper_prep_for_sort(self, hero_id, friends):
        hero_name = self.hero_names[hero_id]
        yield None, (friends, hero_name)

    def reducer_find_max_friends(self, _, friends):
        yield max(friends)


if __name__ == '__main__':
    MostPopularSuperHero.run()
