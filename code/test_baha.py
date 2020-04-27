import os
import apache_beam as beam

p = beam.Pipeline()
my_list = []

lines = (
    p
    | beam.Create({'row1': 5,
                   'row2': 22,
                   'row3': 10})
    # | beam.Map(sorted)
    | beam.io.WriteToText('../data/test')
)
p.run()

# visualize output
print(os.system('head -n 20 ../data/test*'))
