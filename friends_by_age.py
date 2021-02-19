# average number of friends by age of users

from mrjob.job import MRJob


class MRFriendsByAge(MRJob):
    def mapper(self, key, line):
        (user_id, name, age, num_friends) = line.split(',')
        yield age, int(num_friends)

    def reducer(self, age, num_friends):
        total = 0
        num_elements = 0
        for x in num_friends:
            total += x
            num_elements += 1

        yield age, int(total/num_elements)


if __name__ == '__main__':
    MRFriendsByAge.run()
