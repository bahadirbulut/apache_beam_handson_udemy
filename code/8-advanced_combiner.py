import os
import apache_beam as beam

p = beam.Pipeline()


class AverageFn(beam.CombineFn):
    def create_accumulator(self):
        return 0.0, 0  # initialize (sum, count)

    def add_input(self, sum_count, input_, **kwargs):
        (sum_, count_) = sum_count
        return sum_ + input_, count_ + 1

    def merge_accumulators(self, accumulators, **kwargs):
        ind_sums, ind_counts = zip(*accumulators)  # zip -> [(27, 3), (39, 3), (18, 2)]  -->   [(27,39,18), (3,3,2)]
        return sum(ind_sums), sum(ind_counts)      # (84,8)

    def extract_output(self, sum_count, **kwargs):
        (sum_, count) = sum_count  # combine globally using CombineFn
        return sum_ / count if count else float('NaN')   # 84 / 8


small_sum = (
        p
        | beam.Create([15, 5, 7, 7, 9, 23, 13, 5])
        | 'Combine Globally' >> beam.CombineGlobally(AverageFn())
        | 'Write results' >> beam.io.WriteToText('../data/advanced_combine')
)
p.run()

# Sample the first 20 results, remember there are no ordering guarantees.
# !{'head -n 20 data/combine-00000-of-00001'}
print(os.system('cat ../data/advanced_combine*'))
