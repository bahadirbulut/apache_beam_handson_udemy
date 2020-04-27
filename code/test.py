import os
import apache_beam as beam

p = beam.Pipeline()

lines = (
    p
    | beam.Create({'row1': [1, 2, 3, 4, 5],
                   'row2': [1, 2, 3, 4, 5]})
    | beam.Map(lambda element: element)
    | beam.io.WriteToText('../data/test')
)
p.run()

# visualize output
print(os.system('head -n 20 ../data/test*'))
