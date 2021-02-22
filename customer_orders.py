from mrjob.job import MRJob


class MRSpendByCustomer(MRJob):
    def mapper(self, _, line):
        (customer, item, order_amount) = line.split(',')
        yield customer, float(order_amount)

    def reducer(self, customer, order_amount):
        yield customer, sum(order_amount)


if __name__ == '__main__':
    MRSpendByCustomer.run()
