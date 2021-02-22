from mrjob.job import MRJob
from mrjob.step import MRStep


class MRSpendByCustomerSorted(MRJob):
    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_orders,
                   reducer=self.reducer_total),
            MRStep(mapper=self.mapper_make_key,
                   reducer=self.reducer_output_results)
        ]


    def mapper_get_orders(self, _, line):
        (customer, item, order_amount) = line.split(',')
        yield customer, float(order_amount)

    def reducer_total(self, customer, order_amount):
        yield customer, sum(order_amount)

    def mapper_make_key(self, customer, total):
        yield f'{total:04.02f}', customer

    def reducer_output_results(self, total, customers):
        for customer in customers:
            yield customer, total


if __name__ == '__main__':
    MRSpendByCustomerSorted.run()
