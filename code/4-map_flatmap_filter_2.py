import os
import apache_beam as beam


def SplitRow(element):
    return element.split(',')


def filtering(record):
    return record[3] == 'Accounts'


with beam.Pipeline() as p1:
    attendance_count = (
            p1
            | 'Read the file' >> beam.io.ReadFromText('../data/dept_data.txt')
            | 'Split rows' >> beam.Map(SplitRow)
            # | beam.Map(lambda record: record.split(','))

            | 'Filter the data' >> beam.Filter(filtering)
            # |beam.Filter(lambda record: record[3] == 'Accounts')

            | 'Create key, value pair' >> beam.Map(lambda record: (record[1], 1))
            | 'Combine and group' >>  beam.CombinePerKey(sum)

            | 'Write to a file' >> beam.io.WriteToText('../data/map_flatmap_filter_2')
    )

# Sample the first 20 results, remember there are no ordering guarantees.
# !{('head -n 20 data/output_new_final-00000-of-00001')}
print(os.system('cat ../data/map_flatmap_filter_2*'))
