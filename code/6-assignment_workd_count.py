import os
import apache_beam as beam

p = beam.Pipeline()


def split_rows(element):
    return element.split()


def transform_words(element):
    chars = ['[', ']', '(', ')', ',', '.', '--', '?']
    element = element.lower().strip()

    for char in chars:
        element = element.replace(char, '')

    element = (element, 1)

    return element


wordcount = (
        p
        | beam.io.ReadFromText('../data/king_lear.txt')
        | beam.FlatMap(split_rows)
        | beam.Filter(lambda element: element != '')
        | beam.Map(transform_words)
        | beam.CombinePerKey(sum)
        | beam.Filter(lambda keyval: keyval[1] > 50)
        | beam.io.WriteToText('../data/assignment_king_lear_wc')
)
p.run()

print(os.system('cat ../data/assignment_king_lear_wc*'))
