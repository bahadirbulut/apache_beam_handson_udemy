import os
import apache_beam as beam

p1 = beam.Pipeline()

lines1 = (
        p1
        | beam.Create(['Using create transform ',
                       'to generate in memory data ',
                       'This is 3rd line ',
                       'Thanks'])
        | beam.io.WriteToText('../data/outCreate1')
)
p1.run()

# visualize output
# !{('head -n 20 data/outCreate1-00000-of-00001')}
print(os.system('head -n 20 ../data/outCreate1*'))

# -------------------------------------------

p2 = beam.Pipeline()

lines2 = (
        p2
        | beam.Create([1, 2, 3, 4, 5, 6, 7, 8, 9])
        | beam.io.WriteToText('../data/outCreate2')
)
p2.run()

# visualize output
# !{('head -n 20 data/outCreate2-00000-of-00001')}
print(os.system('cat ../data/outCreate2*'))

# -------------------------------------------------

p3 = beam.Pipeline()

lines3 = (
        p3
        | beam.Create([("maths", 52), ("english", 75), ("science", 82), ("computer", 65), ("maths", 85)])
        | beam.io.WriteToText('../data/outCreate3')
)
p3.run()

# visualize output
# !{('head -n 20 data/outCreate3-00000-of-00001')}
print(os.system('cat ../data/outCreate3*'))

# ----------------------------------------------------------------

p4 = beam.Pipeline()

lines4 = (
        p4
        | beam.Create({'row1': [1, 2, 3, 4, 5],
                       'row2': [1, 2, 3, 4, 5]})
        | beam.Map(lambda element: element)
        | beam.io.WriteToText('../data/outCreate4')
)

p4.run()

# visualize output
# !{('head -n 20 data/outCreate4-00000-of-00001')}
print(os.system('cat ../data/outCreate4*'))